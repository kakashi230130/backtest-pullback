/**
 * analyzePullback.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Strategy: Pullback-to-MA (Mean Reversion trong Trend)
 *
 * Ý tưởng cốt lõi:
 *   - HTF (4H + 1D) đang trending rõ ràng
 *   - Giá pullback về MA20 hoặc MA50 trên 1H
 *   - RSI 1H về vùng pullback hợp lý (không phải reversal)
 *   - Candle 15m xác nhận bounce (engulfing hoặc hammer/shooting star)
 *   - Volume 15m spike khi confirm candle xuất hiện
 *   → Vào lệnh tiếp tục theo hướng trend HTF
 *
 * Chạy độc lập với analyzeAndAct.js, dùng chung DB + open_trades table.
 * Không mở lệnh mới nếu đang có PENDING/ACTIVE trade.
 *
 * Env vars (tất cả đều có default, không bắt buộc):
 *   PULLBACK_MA_ZONE_PCT        default 0.004  (0.4% — khoảng cách tối đa từ giá tới MA)
 *   PULLBACK_RSI1H_BULL_MIN     default 35     (RSI 1H tối thiểu khi BULL pullback)
 *   PULLBACK_RSI1H_BULL_MAX     default 55     (RSI 1H tối đa khi BULL pullback)
 *   PULLBACK_RSI1H_BEAR_MIN     default 45     (RSI 1H tối thiểu khi BEAR pullback)
 *   PULLBACK_RSI1H_BEAR_MAX     default 65     (RSI 1H tối đa khi BEAR pullback)
 *   PULLBACK_VOLUME_MULT        default 1.3    (volume 15m confirm phải > avg × mult)
 *   PULLBACK_SL_ATR_MULT        default 0.5    (ATR buffer bên dưới/trên MA50 1H cho SL)
 *   PULLBACK_TP_R_MULT          default 2.0    (TP = entry ± risk × mult)
 *   PULLBACK_MIN_RR             default 1.8    (RR tối thiểu để mở lệnh)
 *   PULLBACK_MAX_CANDLE_STALE   default 900000 (15 phút — data freshness guard, ms)
 *   PULLBACK_REQUIRE_CONFIRM    default 1      (0 = bỏ qua yêu cầu candle confirm)
 *   PULLBACK_USE_MA200          default 1      (1 = cũng check pullback về MA200)
 * ─────────────────────────────────────────────────────────────────────────────
 */

import { pool } from './db.js';
import { logger } from './logger.js';
import { config, tradingEnabled, exchangeName } from './config.js';
import { rsi as calcRsi, sma as calcSma, atr as calcAtr } from './indicators.js';
import {
  setFuturesLeverage,
  placeFuturesLimitOrder,
  placeFuturesStopLossMarket,
  placeFuturesTakeProfitMarket,
  placeFuturesMarketOrder,
  getFuturesOrder,
} from './binanceTradeClient.js';

// ─── Helpers ─────────────────────────────────────────────────────────────────

function last(arr) { return arr[arr.length - 1]; }

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function format(n) {
  if (n == null || Number.isNaN(n)) return null;
  return Number(n).toFixed(8);
}

function pctDist(a, b) {
  if (a == null || b == null || b === 0) return null;
  return Math.abs(a - b) / Math.abs(b);
}

// ─── Candle pattern helpers ───────────────────────────────────────────────────

function candleParts(c) {
  if (!c) return null;
  const open = Number(c.open), close = Number(c.close);
  const high = Number(c.high), low = Number(c.low);
  if (![open, close, high, low].every(Number.isFinite)) return null;
  const body = Math.abs(close - open);
  return {
    open, close, high, low, body,
    upperWick: high - Math.max(open, close),
    lowerWick: Math.min(open, close) - low,
    isBull: close > open,
    isBear: close < open,
  };
}

function isBullishEngulfing(prev, cur) {
  const p = candleParts(prev), c = candleParts(cur);
  if (!p || !c || !p.isBear || !c.isBull) return false;
  return Math.max(c.open, c.close) >= Math.max(p.open, p.close) &&
         Math.min(c.open, c.close) <= Math.min(p.open, p.close);
}

function isBearishEngulfing(prev, cur) {
  const p = candleParts(prev), c = candleParts(cur);
  if (!p || !c || !p.isBull || !c.isBear) return false;
  return Math.max(c.open, c.close) >= Math.max(p.open, p.close) &&
         Math.min(c.open, c.close) <= Math.min(p.open, p.close);
}

function isHammerLike(c) {
  const x = candleParts(c);
  if (!x || x.body <= 0) return false;
  return x.lowerWick >= 2.2 * x.body && x.upperWick <= 0.8 * x.body;
}

function isShootingStarLike(c) {
  const x = candleParts(c);
  if (!x || x.body <= 0) return false;
  return x.upperWick >= 2.2 * x.body && x.lowerWick <= 0.8 * x.body;
}

/**
 * Kiểm tra confirmation candle trên 15m.
 * Nhìn vào 3 candle gần nhất để không bỏ lỡ signal vừa xuất hiện.
 */
function checkCandleConfirm(candles15m, direction) {
  const slice = candles15m.slice(-4);
  if (slice.length < 2) return { ok: false, reason: 'NOT_ENOUGH_CANDLES' };

  const c0 = slice.at(-1);
  const c1 = slice.at(-2);
  const c2 = slice.at(-3);

  if (direction === 'BUY') {
    // Bullish engulfing (cặp bất kỳ trong 3 candle gần nhất)
    if (isBullishEngulfing(c1, c0)) return { ok: true, pattern: 'BULL_ENGULF', tf: '15m', candle: 'last' };
    if (c2 && isBullishEngulfing(c2, c1)) return { ok: true, pattern: 'BULL_ENGULF', tf: '15m', candle: 'prev' };
    // Hammer tại vùng MA
    if (isHammerLike(c0)) return { ok: true, pattern: 'HAMMER', tf: '15m', candle: 'last' };
    if (isHammerLike(c1)) return { ok: true, pattern: 'HAMMER', tf: '15m', candle: 'prev' };
    return { ok: false, reason: 'NO_BULL_CONFIRM' };
  }

  if (direction === 'SELL') {
    if (isBearishEngulfing(c1, c0)) return { ok: true, pattern: 'BEAR_ENGULF', tf: '15m', candle: 'last' };
    if (c2 && isBearishEngulfing(c2, c1)) return { ok: true, pattern: 'BEAR_ENGULF', tf: '15m', candle: 'prev' };
    if (isShootingStarLike(c0)) return { ok: true, pattern: 'SHOOTING_STAR', tf: '15m', candle: 'last' };
    if (isShootingStarLike(c1)) return { ok: true, pattern: 'SHOOTING_STAR', tf: '15m', candle: 'prev' };
    return { ok: false, reason: 'NO_BEAR_CONFIRM' };
  }

  return { ok: false, reason: 'UNKNOWN_DIRECTION' };
}

// ─── Volume check ─────────────────────────────────────────────────────────────

function volumeOk(candles, mult = 1.3, lookback = 20) {
  if (!candles || candles.length < lookback + 2) return { ok: false, reason: 'NOT_ENOUGH_BARS' };
  const cur = candles[candles.length - 1];
  const prev = candles.slice(Math.max(0, candles.length - 1 - lookback), candles.length - 1);
  const curVol = Number(cur?.volume);
  if (!Number.isFinite(curVol)) return { ok: false, reason: 'INVALID_VOL' };
  const vols = prev.map(x => Number(x.volume)).filter(Number.isFinite);
  if (vols.length < 5) return { ok: false, reason: 'NOT_ENOUGH_PREV' };
  const avg = vols.reduce((a, b) => a + b, 0) / vols.length;
  if (!(avg > 0)) return { ok: false, reason: 'ZERO_AVG' };
  return { ok: curVol >= avg * mult, curVol, avg, mult };
}

// ─── DB helpers ───────────────────────────────────────────────────────────────

async function getCandles(symbol, interval, limit = 100) {
  const [rows] = await pool.query(
    `SELECT open_time, open, high, low, close, volume, rsi, ma20, ma50, ma200
     FROM candles
     WHERE symbol=:symbol AND interval_code=:interval
     ORDER BY open_time DESC
     LIMIT ${Number(limit) | 0}`,
    { symbol, interval },
  );
  return rows
    .map(r => ({
      open_time: Number(r.open_time),
      open: Number(r.open), high: Number(r.high),
      low: Number(r.low),  close: Number(r.close),
      volume: Number(r.volume),
      rsi:  r.rsi   == null ? null : Number(r.rsi),
      ma20: r.ma20  == null ? null : Number(r.ma20),
      ma50: r.ma50  == null ? null : Number(r.ma50),
      ma200: r.ma200 == null ? null : Number(r.ma200),
    }))
    .sort((a, b) => a.open_time - b.open_time);
}

async function hasActiveTrade() {
  const [rows] = await pool.query(
    `SELECT id FROM open_trades
     WHERE exchange=:exchange AND status IN ('PENDING','ACTIVE')
     LIMIT 1`,
    { exchange: exchangeName },
  );
  return rows.length > 0;
}

async function updateTrade(id, patch) {
  const cols = Object.keys(patch);
  if (!cols.length) return;
  const sets = cols.map(c => `${c}=:${c}`).join(', ');
  await pool.query(`UPDATE open_trades SET ${sets} WHERE id=:id`, { id, ...patch });
}

// ─── Entry order + DB insert ──────────────────────────────────────────────────

async function openTrade({ symbol, side, entry, sl, tp, qty, notes }) {
  const now = Date.now();
  const entrySide = side === 'LONG' ? 'BUY' : 'SELL';
  const closeSide = side === 'LONG' ? 'SELL' : 'BUY';

  if (!tradingEnabled) {
    logger.warn({ symbol, side, entry, sl, tp, qty }, '[Pullback] TRADING_ENABLED!=1 — dry-run insert');
    await pool.query(
      `INSERT INTO open_trades
         (exchange, symbol, side, leverage, entry_price, quantity, stop_loss, take_profit,
          initial_sl, status, opened_at, notes, last_sync_at)
       VALUES
         (:exchange,:symbol,:side,NULL,:entry,:qty,:sl,:tp,
          :initial_sl,'REJECTED',:now,:notes,:now)`,
      { exchange: exchangeName, symbol, side, entry, qty, sl, tp, initial_sl: sl, now, notes },
    );
    return { entryRes: null, slRes: null, tpRes: null };
  }

  // Place entry order
  const entryClientOrderId = `pb_entry_${symbol}_${now}`.slice(0, 64);
  const scalpEntryMode = String(process.env.SCALP_ENTRY_MODE ?? 'MARKETABLE_LIMIT').toUpperCase();

  let entryRes;
  if (scalpEntryMode === 'MARKET') {
    entryRes = await placeFuturesMarketOrder({
      symbol, side: entrySide, quantity: qty,
      reduceOnly: null, positionSide: null,
      newClientOrderId: entryClientOrderId,
    });
  } else {
    const slipPct = Math.max(0, Math.min(Number(process.env.SCALP_MARKETABLE_LIMIT_SLIP_PCT ?? 0.0002), 0.002));
    const limitPrice = entrySide === 'BUY'
      ? Number(entry) * (1 + slipPct)
      : Number(entry) * (1 - slipPct);
    entryRes = await placeFuturesLimitOrder({
      symbol, side: entrySide, quantity: qty,
      price: limitPrice, newClientOrderId: entryClientOrderId,
    });
  }

  // Poll fill (tái dùng cùng logic với analyzeAndAct)
  let filled = String(entryRes?.status ?? '').toUpperCase() === 'FILLED';
  const allowPoll = (process.env.ENTRY_FILL_POLL_ENABLED ?? '1') === '1';
  const pollMs = Math.max(0, Math.min(Number(process.env.ENTRY_FILL_POLL_MS ?? 6000), 30000));
  const pollStepMs = Math.max(200, Math.min(Number(process.env.ENTRY_FILL_POLL_STEP_MS ?? 500), 2000));

  if (!filled && allowPoll) {
    const until = Date.now() + pollMs;
    while (Date.now() < until) {
      await sleep(pollStepMs);
      try {
        const st = await getFuturesOrder({
          symbol,
          orderId: entryRes?.orderId ?? null,
          origClientOrderId: entryRes?.clientOrderId ?? entryClientOrderId,
        });
        const s = String(st?.status ?? '').toUpperCase();
        if (s === 'FILLED') { filled = true; entryRes = { ...entryRes, status: st.status }; break; }
        if (['CANCELED', 'REJECTED', 'EXPIRED'].includes(s)) break;
      } catch { break; }
    }
  }

  // Place SL/TP nếu entry đã fill
  let slRes = null, tpRes = null;
  const wantFastProtect = (process.env.PLACE_SLTP_FAST_AFTER_FILL ?? '1') === '1';

  if (filled && wantFastProtect) {
    try {
      slRes = await placeFuturesStopLossMarket({
        symbol, side: closeSide, stopPrice: sl,
        quantity: qty, closePosition: true,
        reduceOnly: true, positionSide: null,
        newClientOrderId: `pb_sl_${symbol}_${Date.now()}`.slice(0, 64),
      });
    } catch (err) {
      logger.warn({ err, symbol }, '[Pullback] SL placement failed (ignored)');
    }

    try {
      tpRes = await placeFuturesTakeProfitMarket({
        symbol, side: closeSide, stopPrice: tp,
        quantity: qty, closePosition: true,
        reduceOnly: true, positionSide: null,
        newClientOrderId: `pb_tp_${symbol}_${Date.now()}`.slice(0, 64),
      });
    } catch (err) {
      logger.warn({ err, symbol }, '[Pullback] TP placement failed (ignored)');
    }
  }

  const initialStatus = entryRes?.status === 'FILLED' ? 'ACTIVE' : 'PENDING';

  await pool.query(
    `INSERT INTO open_trades (
       exchange, symbol, side, leverage, entry_price, quantity, stop_loss, take_profit,
       initial_sl, status, opened_at, notes,
       entry_order_id, entry_client_order_id,
       sl_order_id, sl_client_order_id,
       tp_order_id, tp_client_order_id,
       entry_order_status, sl_order_status, tp_order_status,
       last_sync_at
     ) VALUES (
       :exchange,:symbol,:side,NULL,:entry,:qty,:sl,:tp,
       :initial_sl,:status,:now,:notes,
       :entry_order_id,:entry_client_order_id,
       :sl_order_id,:sl_client_order_id,
       :tp_order_id,:tp_client_order_id,
       :entry_order_status,:sl_order_status,:tp_order_status,
       :now
     )`,
    {
      exchange: exchangeName, symbol, side, entry, qty, sl, tp,
      initial_sl: sl, status: initialStatus, now, notes,
      entry_order_id: entryRes?.orderId == null ? null : String(entryRes.orderId),
      entry_client_order_id: entryRes?.clientOrderId ?? entryClientOrderId,
      sl_order_id: slRes?.orderId == null ? null : String(slRes.orderId ?? null),
      sl_client_order_id: slRes?.clientOrderId ?? null,
      tp_order_id: tpRes?.orderId == null ? null : String(tpRes.orderId ?? null),
      tp_client_order_id: tpRes?.clientOrderId ?? null,
      entry_order_status: entryRes?.status ?? null,
      sl_order_status: slRes?.status ?? null,
      tp_order_status: tpRes?.status ?? null,
    },
  );

  return { entryRes, slRes, tpRes };
}

// ─── Core analysis ────────────────────────────────────────────────────────────

async function analyzeSymbolPullback(symbol) {
  // Config từ env
  const maZonePct       = Number(process.env.PULLBACK_MA_ZONE_PCT     ?? 0.004);
  const rsi1hBullMin    = Number(process.env.PULLBACK_RSI1H_BULL_MIN  ?? 35);
  const rsi1hBullMax    = Number(process.env.PULLBACK_RSI1H_BULL_MAX  ?? 55);
  const rsi1hBearMin    = Number(process.env.PULLBACK_RSI1H_BEAR_MIN  ?? 45);
  const rsi1hBearMax    = Number(process.env.PULLBACK_RSI1H_BEAR_MAX  ?? 65);
  const volMult         = Number(process.env.PULLBACK_VOLUME_MULT     ?? 1.3);
  const slAtrMult       = Number(process.env.PULLBACK_SL_ATR_MULT     ?? 0.5);
  const tpRMult         = Number(process.env.PULLBACK_TP_R_MULT       ?? 2.0);
  const minRr           = Number(process.env.PULLBACK_MIN_RR          ?? 1.8);
  const maxStaleMs      = Number(process.env.PULLBACK_MAX_CANDLE_STALE ?? 15 * 60 * 1000);
  const requireConfirm  = (process.env.PULLBACK_REQUIRE_CONFIRM ?? '1') === '1';
  const useMA200        = (process.env.PULLBACK_USE_MA200       ?? '1') === '1';

  // Lấy candles
  const c1d  = await getCandles(symbol, '1d', 60);
  const c4h  = await getCandles(symbol, '4h', 60);
  const c1h  = await getCandles(symbol, '1h', 80);
  const c15m = await getCandles(symbol, '15m', 60);

  const last1d  = last(c1d);
  const last4h  = last(c4h);
  const last1h  = last(c1h);
  const last15m = last(c15m);

  // ── Freshness guard ──────────────────────────────────────────────────────
  if (!last15m) return { skip: true, reason: 'NO_15M_DATA' };
  const ageMs = Date.now() - last15m.open_time;
  if (maxStaleMs > 0 && ageMs > maxStaleMs) {
    return { skip: true, reason: 'STALE_DATA', ageMs };
  }

  // ── Bước 1: HTF Trend filter (4H + 1D) ──────────────────────────────────
  // Dùng MA từ DB (đã tính sẵn trong syncCandles)
  // BULL: giá > MA50, MA20 > MA50 trên 4H VÀ 1D
  // BEAR: giá < MA50, MA20 < MA50 trên 4H VÀ 1D

  function trendOf(c) {
    if (!c || c.ma20 == null || c.ma50 == null) return 'NEUTRAL';
    if (c.close > c.ma50 && c.ma20 > c.ma50) return 'BULL';
    if (c.close < c.ma50 && c.ma20 < c.ma50) return 'BEAR';
    return 'NEUTRAL';
  }

  const trend4h = trendOf(last4h);
  const trend1d = trendOf(last1d);

  // Cần cả 4H lẫn 1D đồng thuận → tránh vào lệnh khi HTF mâu thuẫn
  let htfTrend = 'NEUTRAL';
  if (trend4h === 'BULL' && trend1d === 'BULL') htfTrend = 'BULL';
  else if (trend4h === 'BEAR' && trend1d === 'BEAR') htfTrend = 'BEAR';

  if (htfTrend === 'NEUTRAL') {
    return { skip: false, signal: false, reason: 'HTF_NEUTRAL', trend4h, trend1d };
  }

  const direction = htfTrend === 'BULL' ? 'BUY' : 'SELL';

  // ── Bước 2: Pullback detection trên 1H ──────────────────────────────────
  // Giá phải đang gần MA20, MA50, hoặc (nếu bật) MA200 trên 1H
  // "Gần" = trong vòng maZonePct %

  if (!last1h) return { skip: true, reason: 'NO_1H_DATA' };

  const close1h = last1h.close;
  const ma20_1h = last1h.ma20;
  const ma50_1h = last1h.ma50;
  const ma200_1h = last1h.ma200;

  const dMA20  = ma20_1h  != null ? pctDist(close1h, ma20_1h)  : null;
  const dMA50  = ma50_1h  != null ? pctDist(close1h, ma50_1h)  : null;
  const dMA200 = ma200_1h != null ? pctDist(close1h, ma200_1h) : null;

  const nearMA20  = dMA20  != null && dMA20  <= maZonePct;
  const nearMA50  = dMA50  != null && dMA50  <= maZonePct;
  const nearMA200 = useMA200 && dMA200 != null && dMA200 <= maZonePct;

  const nearAnyMA = nearMA20 || nearMA50 || nearMA200;
  const whichMA   = nearMA200 ? 'MA200' : (nearMA50 ? 'MA50' : 'MA20');

  if (!nearAnyMA) {
    return {
      skip: false, signal: false, reason: 'NOT_AT_MA_ZONE',
      dMA20, dMA50, dMA200, maZonePct,
    };
  }

  // Thêm điều kiện: trong BULL trend, giá phải đến từ trên và đang TRÊN MA50
  // (tránh case giá đã xuyên qua MA50 và đang phục hồi từ dưới — đó là reversal, không phải pullback)
  if (direction === 'BUY'  && ma50_1h != null && close1h < ma50_1h * 0.995) {
    return { skip: false, signal: false, reason: 'BULL_PRICE_BELOW_MA50' };
  }
  if (direction === 'SELL' && ma50_1h != null && close1h > ma50_1h * 1.005) {
    return { skip: false, signal: false, reason: 'BEAR_PRICE_ABOVE_MA50' };
  }

  // ── Bước 3: RSI 1H trong vùng pullback hợp lý ───────────────────────────
  // Tái tính RSI từ raw closes để chính xác hơn DB value
  const closes1h = c1h.map(c => c.close);
  const rsiArr1h = calcRsi(closes1h, 14);
  const rsi1h = rsiArr1h[rsiArr1h.length - 1];

  if (rsi1h == null) {
    return { skip: false, signal: false, reason: 'NO_RSI_1H' };
  }

  let rsiOk = false;
  if (direction === 'BUY'  && rsi1h >= rsi1hBullMin && rsi1h <= rsi1hBullMax) rsiOk = true;
  if (direction === 'SELL' && rsi1h >= rsi1hBearMin && rsi1h <= rsi1hBearMax) rsiOk = true;

  if (!rsiOk) {
    return {
      skip: false, signal: false, reason: 'RSI_OUT_OF_ZONE',
      rsi1h, direction,
      expected: direction === 'BUY'
        ? `${rsi1hBullMin}–${rsi1hBullMax}`
        : `${rsi1hBearMin}–${rsi1hBearMax}`,
    };
  }

  // ── Bước 4: Candle confirmation trên 15m ────────────────────────────────
  let confirmResult = { ok: true, pattern: 'SKIPPED' };
  if (requireConfirm) {
    confirmResult = checkCandleConfirm(c15m, direction);
    if (!confirmResult.ok) {
      return { skip: false, signal: false, reason: confirmResult.reason, direction };
    }
  }

  // ── Bước 5: Volume filter trên 15m ──────────────────────────────────────
  const volResult = volumeOk(c15m, volMult, 20);
  if (!volResult.ok) {
    return {
      skip: false, signal: false, reason: `VOLUME_FILTER: ${volResult.reason}`,
      volResult,
    };
  }

  // ── Tính SL / TP ─────────────────────────────────────────────────────────
  const entry = last15m.close;

  // ATR 1H để tính buffer cho SL
  const highs1h  = c1h.map(c => c.high);
  const lows1h   = c1h.map(c => c.low);
  const atrArr1h = calcAtr(highs1h, lows1h, closes1h, 14);
  const atr1h    = atrArr1h[atrArr1h.length - 1];
  const slBuffer = atr1h != null ? atr1h * Math.max(0, slAtrMult) : 0;

  let sl, tp, risk;

  if (direction === 'BUY') {
    // SL dưới MA50 1H + ATR buffer (MA50 là support chính trong pullback)
    const slBase = ma50_1h ?? (entry * 0.995);
    sl = slBase - slBuffer;
    risk = entry - sl;
    if (risk <= 0) return { skip: false, signal: false, reason: 'INVALID_RISK_BUY', entry, sl };
    tp = entry + tpRMult * risk;
  } else {
    // SL trên MA50 1H + ATR buffer
    const slBase = ma50_1h ?? (entry * 1.005);
    sl = slBase + slBuffer;
    risk = sl - entry;
    if (risk <= 0) return { skip: false, signal: false, reason: 'INVALID_RISK_SELL', entry, sl };
    tp = entry - tpRMult * risk;
  }

  const rr = tpRMult; // cố định theo config, thực tế = (tp - entry) / risk

  // ── RR filter ────────────────────────────────────────────────────────────
  if (rr < minRr) {
    return { skip: false, signal: false, reason: 'RR_TOO_LOW', rr, minRr };
  }

  // ── Signal hợp lệ ────────────────────────────────────────────────────────
  return {
    skip: false,
    signal: true,
    direction,
    entry,
    sl,
    tp,
    rr,
    whichMA,
    rsi1h,
    confirmResult,
    volResult,
    meta: {
      trend4h, trend1d, htfTrend,
      close1h, ma20_1h, ma50_1h, ma200_1h,
      dMA20, dMA50, dMA200,
      atr1h, slBuffer,
    },
  };
}

// ─── parseSymbolQtyMap (tái dùng từ analyzeAndAct) ────────────────────────────

function parseSymbolQtyMap(raw) {
  const map = {};
  if (!raw) return map;
  for (const part of raw.split(',')) {
    const p = part.trim();
    if (!p) continue;
    const [sym, qty] = p.split(':').map(x => x.trim());
    if (!sym || !qty) continue;
    const q = Number(qty);
    if (!Number.isFinite(q) || q <= 0) continue;
    map[sym] = q;
  }
  return map;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const runId = `pullback:${Date.now()}`;
  const startedAt = Date.now();
  logger.info({ runId }, '[Pullback] Start');

  // Không mở lệnh mới nếu đang có trade active
  if (await hasActiveTrade()) {
    logger.info({ runId }, '[Pullback] Active/pending trade exists — skip');
    console.log(JSON.stringify({ runId, action: 'SKIP', reason: 'ACTIVE_TRADE_EXISTS' }, null, 2));
    return;
  }

  const symbols = [...config.symbols];
  const qtyMap  = parseSymbolQtyMap(process.env.SYMBOL_QTY_MAP);

  const results = [];

  for (const symbol of symbols) {
    const r = await analyzeSymbolPullback(symbol);
    results.push({ symbol, ...r });

    if (r.skip) {
      logger.info({ runId, symbol, reason: r.reason }, '[Pullback] Skip symbol');
      continue;
    }

    if (!r.signal) {
      logger.info(
        { runId, symbol, reason: r.reason, direction: r.direction },
        '[Pullback] No signal',
      );
      continue;
    }

    // Signal hợp lệ — kiểm tra qty
    const qty = qtyMap[symbol] ?? 0;
    if (!qty) {
      logger.warn({ runId, symbol }, '[Pullback] No qty configured — set SYMBOL_QTY_MAP');
      results[results.length - 1].action = 'WAIT_NO_QTY';
      continue;
    }

    // Double-check lần nữa trước khi mở (tránh race condition nếu analyzeAndAct chạy cùng lúc)
    if (await hasActiveTrade()) {
      logger.info({ runId, symbol }, '[Pullback] Trade appeared between checks — abort');
      results[results.length - 1].action = 'ABORT_RACE';
      continue;
    }

    logger.info(
      { runId, symbol, direction: r.direction, entry: r.entry, sl: r.sl, tp: r.tp, rr: r.rr, whichMA: r.whichMA },
      '[Pullback] Opening trade',
    );

    const side = r.direction === 'BUY' ? 'LONG' : 'SHORT';
    const notes = `PULLBACK_${r.direction} @${r.whichMA} rsi1h=${r.rsi1h?.toFixed(1)} pattern=${r.confirmResult?.pattern} rr=${r.rr.toFixed(2)}`.slice(0, 255);

    try {
      const { entryRes, slRes, tpRes } = await openTrade({
        symbol, side,
        entry: format(r.entry),
        sl:    format(r.sl),
        tp:    format(r.tp),
        qty:   format(qty),
        notes,
      });

      results[results.length - 1].action = 'OPEN';
      results[results.length - 1].orderIds = {
        entry: entryRes?.orderId ?? null,
        sl:    slRes?.orderId    ?? null,
        tp:    tpRes?.orderId    ?? null,
      };

      logger.info({ runId, symbol, side, entryStatus: entryRes?.status }, '[Pullback] Trade opened');

      // Chỉ mở 1 lệnh per run
      break;
    } catch (err) {
      logger.error({ err, runId, symbol }, '[Pullback] openTrade failed');
      results[results.length - 1].action = 'ERROR';
      results[results.length - 1].error = String(err?.message ?? err);
    }
  }

  const payload = {
    runId,
    ms: Date.now() - startedAt,
    results,
  };

  logger.info({ runId, ms: payload.ms, results: results.map(r => ({ symbol: r.symbol, signal: r.signal, reason: r.reason, action: r.action })) }, '[Pullback] Done');
  console.log(JSON.stringify(payload, null, 2));

  if (process.env.CLOSE_DB_POOL === '1') {
    await pool.end();
  }
}

try {
  await main();
} catch (err) {
  logger.error({ err }, '[Pullback] Fatal error');
  throw err;
}
