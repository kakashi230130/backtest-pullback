import { pool } from './db.js';
import { logger } from './logger.js';
import { config, tradingEnabled, exchangeName } from './config.js';
import { adx as calcAdx, rsi as calcRsi, sma as calcSma, atr as calcAtr } from './indicators.js';
import {
  setFuturesLeverage,
  placeFuturesLimitOrder,
  placeFuturesStopLossMarket,
  placeFuturesTakeProfitMarket,
  cancelFuturesOrder,
  placeFuturesMarketOrder,
} from './binanceTradeClient.js';

const INTERVALS = ['5m', '15m', '30m', '1h', '4h', '1d'];

function sideHitSlTp({ side, price, sl, tp }) {
  if (side === 'LONG') {
    if (sl != null && price <= sl) return { hit: 'SL' };
    if (tp != null && price >= tp) return { hit: 'TP' };
  } else {
    if (sl != null && price >= sl) return { hit: 'SL' };
    if (tp != null && price <= tp) return { hit: 'TP' };
  }
  return { hit: null };
}

function trendLabel({ close, ma20, ma50, rsi }) {
  if (ma20 == null || ma50 == null || rsi == null) return 'NEUTRAL';
  if (close > ma50 && ma20 > ma50 && rsi >= 50) return 'BULL';
  if (close < ma50 && ma20 < ma50 && rsi <= 50) return 'BEAR';
  return 'NEUTRAL';
}

function structureBreak({ tradeSide, htf }) {
  // Strict definition to avoid over-reacting.
  const { close, ma20, ma50, rsi } = htf;
  if (ma20 == null || ma50 == null || rsi == null) return false;

  if (tradeSide === 'LONG') {
    return close < ma50 && ma20 < ma50 && rsi < 45;
  }
  return close > ma50 && ma20 > ma50 && rsi > 55;
}

async function getCandles(symbol, interval, limit = 260) {
  const [rows] = await pool.query(
    `
    SELECT open_time, open, high, low, close, volume, rsi, ma20, ma50, ma200
    FROM candles
    WHERE symbol=:symbol AND interval_code=:interval
    ORDER BY open_time DESC
    LIMIT ${Number(limit) | 0}
    `,
    { symbol, interval },
  );

  return rows
    .map(r => ({
      open_time: Number(r.open_time),
      open: Number(r.open),
      high: Number(r.high),
      low: Number(r.low),
      close: Number(r.close),
      volume: Number(r.volume),
      rsi: r.rsi == null ? null : Number(r.rsi),
      ma20: r.ma20 == null ? null : Number(r.ma20),
      ma50: r.ma50 == null ? null : Number(r.ma50),
      ma200: r.ma200 == null ? null : Number(r.ma200),
    }))
    .sort((a, b) => a.open_time - b.open_time);
}

function last(arr) {
  return arr[arr.length - 1];
}

function recentSwingLow(candles, lookback = 20) {
  const slice = candles.slice(Math.max(0, candles.length - lookback));
  return Math.min(...slice.map(c => c.low));
}

function recentSwingHigh(candles, lookback = 20) {
  const slice = candles.slice(Math.max(0, candles.length - lookback));
  return Math.max(...slice.map(c => c.high));
}

function format(n) {
  if (n == null || Number.isNaN(n)) return null;
  return Number(n).toFixed(8);
}

async function getActiveTrades() {
  const [rows] = await pool.query(
    `SELECT * FROM open_trades WHERE status IN ('PENDING','ACTIVE') ORDER BY id DESC`,
  );
  return rows.map(r => ({
    id: Number(r.id),
    exchange: r.exchange,
    symbol: r.symbol,
    side: r.side,
    leverage: r.leverage == null ? null : Number(r.leverage),
    entry_price: Number(r.entry_price),
    quantity: Number(r.quantity),
    stop_loss: r.stop_loss == null ? null : Number(r.stop_loss),
    take_profit: r.take_profit == null ? null : Number(r.take_profit),
    status: r.status,
    opened_at: r.opened_at == null ? null : Number(r.opened_at),
    notes: r.notes ?? null,

    entry_order_id: r.entry_order_id == null ? null : Number(r.entry_order_id),
    sl_order_id: r.sl_order_id == null ? null : Number(r.sl_order_id),
    tp_order_id: r.tp_order_id == null ? null : Number(r.tp_order_id),
  }));
}

async function getLastClosedTradeForSymbol(symbol) {
  const [[row]] = await pool.query(
    `
    SELECT *
    FROM open_trades
    WHERE symbol=:symbol AND status='CLOSED'
    ORDER BY id DESC
    LIMIT 1
    `,
    { symbol },
  );
  if (!row) return null;
  return {
    id: Number(row.id),
    symbol: row.symbol,
    side: row.side,
    status: row.status,
    opened_at: row.opened_at == null ? null : Number(row.opened_at),
    closed_at: row.closed_at == null ? null : Number(row.closed_at),
    notes: row.notes ?? null,
    entry_price: row.entry_price == null ? null : Number(row.entry_price),
    stop_loss: row.stop_loss == null ? null : Number(row.stop_loss),
    take_profit: row.take_profit == null ? null : Number(row.take_profit),
  };
}

function candleParts(c) {
  if (!c) return null;
  const open = Number(c.open);
  const close = Number(c.close);
  const high = Number(c.high);
  const low = Number(c.low);
  if (![open, close, high, low].every(Number.isFinite)) return null;
  const body = Math.abs(close - open);
  const upperWick = high - Math.max(open, close);
  const lowerWick = Math.min(open, close) - low;
  return {
    open,
    close,
    high,
    low,
    body,
    upperWick,
    lowerWick,
    isBull: close > open,
    isBear: close < open,
  };
}

function isBearishEngulfing(prev, cur) {
  const p = candleParts(prev);
  const c = candleParts(cur);
  if (!p || !c) return false;
  if (!p.isBull || !c.isBear) return false;
  const prevBodyHigh = Math.max(p.open, p.close);
  const prevBodyLow = Math.min(p.open, p.close);
  const curBodyHigh = Math.max(c.open, c.close);
  const curBodyLow = Math.min(c.open, c.close);
  return curBodyHigh >= prevBodyHigh && curBodyLow <= prevBodyLow;
}

function isBullishEngulfing(prev, cur) {
  const p = candleParts(prev);
  const c = candleParts(cur);
  if (!p || !c) return false;
  if (!p.isBear || !c.isBull) return false;
  const prevBodyHigh = Math.max(p.open, p.close);
  const prevBodyLow = Math.min(p.open, p.close);
  const curBodyHigh = Math.max(c.open, c.close);
  const curBodyLow = Math.min(c.open, c.close);
  return curBodyHigh >= prevBodyHigh && curBodyLow <= prevBodyLow;
}

function isShootingStarLike(c) {
  const x = candleParts(c);
  if (!x) return false;
  // long upper wick + small body near lows, typically bearish rejection
  if (x.body <= 0) return false;
  return x.upperWick >= 2.2 * x.body && x.lowerWick <= 0.8 * x.body;
}

function isHammerLike(c) {
  const x = candleParts(c);
  if (!x) return false;
  if (x.body <= 0) return false;
  return x.lowerWick >= 2.2 * x.body && x.upperWick <= 0.8 * x.body;
}

function reversalConfirmForContinuation({ bias, candles15m, candles30m, candles1h }) {
  // We use the last completed candles on 15m/30m/1h.
  // Goal: after a stop-out, require a clear reversal candle back in the HTF direction
  // to avoid immediate re-entry in the same choppy zone.
  const m15 = candles15m?.length >= 2 ? candles15m[candles15m.length - 1] : null;
  const m15Prev = candles15m?.length >= 3 ? candles15m[candles15m.length - 2] : null;
  const m30 = candles30m?.length >= 2 ? candles30m[candles30m.length - 1] : null;
  const m30Prev = candles30m?.length >= 3 ? candles30m[candles30m.length - 2] : null;
  const h1 = candles1h?.length >= 2 ? candles1h[candles1h.length - 1] : null;
  const h1Prev = candles1h?.length >= 3 ? candles1h[candles1h.length - 2] : null;

  if (bias === 'SELL') {
    const bearEngulf =
      isBearishEngulfing(m15Prev, m15) ||
      isBearishEngulfing(m30Prev, m30) ||
      isBearishEngulfing(h1Prev, h1);

    const rejectUp = isShootingStarLike(m15) || isShootingStarLike(m30) || isShootingStarLike(h1);

    return {
      ok: bearEngulf || rejectUp,
      why: bearEngulf ? 'BEAR_ENGULF' : (rejectUp ? 'UPPER_WICK_REJECT' : 'NO_REVERSAL'),
      tf: bearEngulf
        ? (isBearishEngulfing(m15Prev, m15) ? '15m' : (isBearishEngulfing(m30Prev, m30) ? '30m' : '1h'))
        : (rejectUp ? (isShootingStarLike(m15) ? '15m' : (isShootingStarLike(m30) ? '30m' : '1h')) : null),
    };
  }

  if (bias === 'BUY') {
    const bullEngulf =
      isBullishEngulfing(m15Prev, m15) ||
      isBullishEngulfing(m30Prev, m30) ||
      isBullishEngulfing(h1Prev, h1);

    const rejectDown = isHammerLike(m15) || isHammerLike(m30) || isHammerLike(h1);

    return {
      ok: bullEngulf || rejectDown,
      why: bullEngulf ? 'BULL_ENGULF' : (rejectDown ? 'LOWER_WICK_REJECT' : 'NO_REVERSAL'),
      tf: bullEngulf
        ? (isBullishEngulfing(m15Prev, m15) ? '15m' : (isBullishEngulfing(m30Prev, m30) ? '30m' : '1h'))
        : (rejectDown ? (isHammerLike(m15) ? '15m' : (isHammerLike(m30) ? '30m' : '1h')) : null),
    };
  }

  return { ok: false, why: 'BIAS_WAIT', tf: null };
}

function isStopLossClose(notes) {
  const s = String(notes ?? '').toUpperCase();
  return s.includes('SL_FILLED') || s.includes('HIT_SL') || s.includes('STOP_LOSS');
}

function shouldBlockReentryAfterStopout({ lastClosedTrade, bias, trends, reversalConfirm }) {
  const enabled = (process.env.REENTRY_AFTER_SL_REQUIRE_REVERSAL ?? '1') === '1';
  if (!enabled) return { block: false };
  if (!lastClosedTrade) return { block: false };
  if (!isStopLossClose(lastClosedTrade.notes)) return { block: false };

  // Only gate when 1D+4H are clearly trending and we want to re-enter the same direction.
  const htfDown = trends?.trend1d === 'BEAR' && trends?.trend4h === 'BEAR' && bias === 'SELL';
  const htfUp = trends?.trend1d === 'BULL' && trends?.trend4h === 'BULL' && bias === 'BUY';

  if (!htfDown && !htfUp) return { block: false };

  const sameDir = (bias === 'SELL' && lastClosedTrade.side === 'SHORT') || (bias === 'BUY' && lastClosedTrade.side === 'LONG');
  if (!sameDir) return { block: false };

  // Optional cooldown window to avoid instant re-entry in the same hour.
  const cooldownMin = Number(process.env.REENTRY_AFTER_SL_COOLDOWN_MIN ?? 0);
  if (cooldownMin > 0 && lastClosedTrade.closed_at) {
    const ageMin = (Date.now() - lastClosedTrade.closed_at) / 60000;
    if (ageMin < cooldownMin) {
      return { block: true, reason: 'SL_COOLDOWN', details: { ageMin, cooldownMin } };
    }
  }

  if (!reversalConfirm?.ok) {
    return { block: true, reason: 'WAIT_REVERSAL_CANDLE', details: { reversalWhy: reversalConfirm?.why ?? null } };
  }

  return { block: false };
}

async function updateTrade(id, patch) {
  const cols = Object.keys(patch);
  if (!cols.length) return;
  const sets = cols.map(c => `${c}=:${c}`).join(', ');
  await pool.query(`UPDATE open_trades SET ${sets} WHERE id=:id`, { id, ...patch });
}

async function cancelAndCloseTradeOnExchange(trade, reason, price) {
  // Cancel any outstanding orders (best-effort)
  for (const oid of [trade.entry_order_id, trade.sl_order_id, trade.tp_order_id]) {
    if (!oid) continue;
    try {
      await cancelFuturesOrder({ symbol: trade.symbol, orderId: oid });
    } catch (err) {
      logger.warn({ err, id: trade.id, symbol: trade.symbol, orderId: oid }, 'Cancel order failed (ignored)');
    }
  }

  // If it was already ACTIVE, also flatten position with a market order (best-effort).
  // We use the trade.quantity as intended size.
  if (trade.status === 'ACTIVE' && trade.quantity) {
    try {
      await placeFuturesMarketOrder({
        symbol: trade.symbol,
        side: trade.side === 'LONG' ? 'SELL' : 'BUY',
        quantity: trade.quantity,
      });
    } catch (err) {
      logger.warn({ err, id: trade.id, symbol: trade.symbol }, 'Market close failed (ignored)');
    }
  }

  await updateTrade(trade.id, {
    status: 'CLOSED',
    closed_at: Date.now(),
    notes: `${reason} @${price}`.slice(0, 255),
  });
}

async function insertPendingTradeWithOrders({ symbol, side, entry, sl, tp, qty, leverage = null, notes = null }) {
  const now = Date.now();

  // Configure leverage (optional)
  if (leverage != null) {
    try {
      await setFuturesLeverage({ symbol, leverage });
    } catch (err) {
      logger.warn({ err, symbol, leverage }, 'Set leverage failed (ignored)');
    }
  }

  const entrySide = side === 'LONG' ? 'BUY' : 'SELL';
  const closeSide = side === 'LONG' ? 'SELL' : 'BUY';

  if (!tradingEnabled) {
    logger.warn({ symbol, side, entry, sl, tp, qty }, 'TRADING_ENABLED!=1, skipping order placement (dry-run)');
    // Insert as REJECTED so it is visible in DB but won't be watched/acted on.
    await pool.query(
      `
      INSERT INTO open_trades (
        exchange,
        symbol, side, leverage, entry_price, quantity, stop_loss, take_profit,
        status, opened_at, notes, last_sync_at
      )
      VALUES (:exchange,:symbol,:side,:leverage,:entry,:qty,:sl,:tp,'REJECTED',:now,:notes,:now)
      `,
      {
        exchange: exchangeName,
        symbol,
        side,
        leverage,
        entry,
        qty,
        sl,
        tp,
        now,
        notes: `DRY_RUN AUTO_OPEN ${side}`.slice(0, 255),
      },
    );
    return { entryRes: null, slRes: null, tpRes: null };
  }

  // Create entry on Binance
  // IMPORTANT: always generate a clientOrderId (string) so watcher can safely query even when orderId > 2^53.
  const entryClientOrderId = `oc_entry_${symbol}_${Date.now()}`.slice(0, 64);
  const entryRes = await placeFuturesLimitOrder({
    symbol,
    side: entrySide,
    quantity: qty,
    price: entry,
    newClientOrderId: entryClientOrderId,
  });

  // NOTE: Do NOT place SL/TP here.
  // Rationale: if SL/TP placement errors, we would fail before inserting the trade into DB.
  // We keep analyze responsible for ENTRY + DB insert; watcher will place SL/TP after entry is FILLED.
  const slRes = null;
  const tpRes = null;

  await pool.query(
    `
    INSERT INTO open_trades (
      exchange,
      symbol, side, leverage, entry_price, quantity, stop_loss, take_profit,
      status, opened_at, notes,
      entry_order_id, entry_client_order_id,
      sl_order_id, sl_client_order_id,
      tp_order_id, tp_client_order_id,
      entry_order_status, sl_order_status, tp_order_status,
      last_sync_at
    )
    VALUES (
      :exchange,
      :symbol, :side, :leverage, :entry, :qty, :sl, :tp,
      'PENDING', :now, :notes,
      :entry_order_id, :entry_client_order_id,
      :sl_order_id, :sl_client_order_id,
      :tp_order_id, :tp_client_order_id,
      :entry_order_status, :sl_order_status, :tp_order_status,
      :now
    )
    `,
    {
      exchange: exchangeName,
      symbol,
      side,
      leverage,
      entry,
      qty,
      sl,
      tp,
      now,
      notes: notes?.slice(0, 255) ?? null,
      // Keep orderId as string (may be > 2^53)
      entry_order_id: entryRes?.orderId == null ? null : String(entryRes.orderId),
      entry_client_order_id: entryRes?.clientOrderId ?? entryClientOrderId,

      sl_order_id: slRes?.orderId == null ? null : String(slRes.orderId),
      sl_client_order_id: slRes?.clientOrderId ?? null,

      tp_order_id: tpRes?.orderId == null ? null : String(tpRes.orderId),
      tp_client_order_id: tpRes?.clientOrderId ?? null,

      entry_order_status: entryRes.status ?? null,
      sl_order_status: slRes?.status ?? null,
      tp_order_status: tpRes?.status ?? null,
    },
  );

  return { entryRes, slRes, tpRes };
}

function decideBias(t1, t2, t3) {
  const arr = [t1, t2, t3];
  const bull = arr.filter(x => x === 'BULL').length;
  const bear = arr.filter(x => x === 'BEAR').length;
  if (bull >= 2) return 'BUY';
  if (bear >= 2) return 'SELL';
  return 'WAIT';
}

function pctDist(a, b) {
  if (a == null || b == null || b === 0) return null;
  return Math.abs(a - b) / Math.abs(b);
}

function inMaZone(c, pct = 0.006) {
  // near MA20/MA50 zone OR near MA200
  if (!c) return false;
  const d20 = c.ma20 == null ? null : pctDist(c.close, c.ma20);
  const d50 = c.ma50 == null ? null : pctDist(c.close, c.ma50);
  const d200 = c.ma200 == null ? null : pctDist(c.close, c.ma200);
  const near20or50 = (d20 != null && d20 <= pct) || (d50 != null && d50 <= pct);
  const near200 = d200 != null && d200 <= pct;
  return near20or50 || near200;
}

function maSlope(maArr, bars = 10) {
  // simple slope as % change over N bars
  const n = maArr.length;
  if (n < bars + 1) return null;
  const a = maArr[n - 1];
  const b = maArr[n - 1 - bars];
  if (a == null || b == null || b === 0) return null;
  return (a - b) / b;
}

function isSideways({ adxNow, slope50, rangePct }) {
  // conservative filter: any sign of chop => sideways
  if (adxNow != null && adxNow < 18) return true;
  if (slope50 != null && Math.abs(slope50) < 0.002) return true; // <0.2% over ~10 bars
  if (rangePct != null && rangePct < 0.006) return true; // <0.6% range in window
  return false;
}

function swingPoints(candles, left = 2, right = 2) {
  const highs = [];
  const lows = [];
  for (let i = left; i < candles.length - right; i++) {
    const hi = candles[i].high;
    const lo = candles[i].low;
    let isHigh = true;
    let isLow = true;
    for (let j = i - left; j <= i + right; j++) {
      if (j === i) continue;
      if (candles[j].high >= hi) isHigh = false;
      if (candles[j].low <= lo) isLow = false;
    }
    if (isHigh) highs.push({ idx: i, price: hi });
    if (isLow) lows.push({ idx: i, price: lo });
  }
  return { highs, lows };
}

function structureLabel(candles) {
  const { highs, lows } = swingPoints(candles);
  if (highs.length < 2 || lows.length < 2) return 'RANGE';
  const h1 = highs[highs.length - 1];
  const h0 = highs[highs.length - 2];
  const l1 = lows[lows.length - 1];
  const l0 = lows[lows.length - 2];

  const bullish = h1.price > h0.price && l1.price > l0.price;
  const bearish = h1.price < h0.price && l1.price < l0.price;
  if (bullish) return 'BULL';
  if (bearish) return 'BEAR';
  return 'RANGE';
}

function rsiDivergence({ candles, rsiArr, type }) {
  // Detect divergence based on last two swing highs/lows
  const { highs, lows } = swingPoints(candles);
  if (type === 'BULL') {
    if (lows.length < 2) return { ok: false };
    const a = lows[lows.length - 2];
    const b = lows[lows.length - 1];
    const rA = rsiArr[a.idx];
    const rB = rsiArr[b.idx];
    if (rA == null || rB == null) return { ok: false };
    // price lower low, RSI higher low
    const ok = b.price < a.price && rB > rA;
    return { ok, a, b, rA, rB };
  }
  if (type === 'BEAR') {
    if (highs.length < 2) return { ok: false };
    const a = highs[highs.length - 2];
    const b = highs[highs.length - 1];
    const rA = rsiArr[a.idx];
    const rB = rsiArr[b.idx];
    if (rA == null || rB == null) return { ok: false };
    // price higher high, RSI lower high
    const ok = b.price > a.price && rB < rA;
    return { ok, a, b, rA, rB };
  }
  return { ok: false };
}

function isEntrySignalV2({ bias, candles30, candles15, candles5 }) {
  const profile = (process.env.ANALYZE_PROFILE ?? 'strict').toLowerCase();
  // New strict rules (default):
  // - HTF bias must be BUY/SELL (already derived from 1D/4H/1H)
  // - Mid-timeframe structure (30m OR 15m) must align (HH/HL for BUY, LH/LL for SELL)
  // - Avoid sideways: ADX low OR MA slope flat OR range compressed
  // - Require RSI divergence at a MA zone (MA20/MA50/MA200) on 15m
  // - LTF confirmation on 5m: RSI momentum in direction (>=52 for BUY, <=48 for SELL)
  //
  // TEMP DEBUG MODE:
  // If ANALYZE_EASY_MODE=1, we deliberately relax the entry rules so you can
  // verify the end-to-end analyze → create-trade flow works.
  // (This is NOT intended for real trading.)

  if (bias !== 'BUY' && bias !== 'SELL') return { ok: false, reason: 'BIAS_WAIT' };

  const easyMode = process.env.ANALYZE_EASY_MODE === '1';
  if (easyMode) {
    const c5 = last(candles5);
    // Optional lightweight LTF check (comment out if you want it even easier)
    if (c5?.rsi == null) return { ok: false, reason: 'EASY_NO_RSI_5M' };
    if (bias === 'BUY' && c5.rsi < 50) return { ok: false, reason: 'EASY_NO_MOMENTUM' };
    if (bias === 'SELL' && c5.rsi > 50) return { ok: false, reason: 'EASY_NO_MOMENTUM' };

    // We skip: sideways filter, structure alignment, divergence, MA-zone checks.
    return { ok: true, mode: 'EASY', note: 'ANALYZE_EASY_MODE=1 (relaxed rules)' };
  }

  // SCALP profile: loosen filters to get higher trade frequency.
  // - Do NOT require divergence
  // - Do NOT require MA-zone
  // - Sideways filter is much looser
  // - Structure is used as a "don't trade against obvious structure" filter
  if (profile === 'scalp') {
    const c30 = last(candles30);
    const c15 = last(candles15);
    const c5 = last(candles5);

    const closes15 = candles15.map(c => c.close);
    const highs15 = candles15.map(c => c.high);
    const lows15 = candles15.map(c => c.low);

    const rsi15 = calcRsi(closes15, 14);
    const adx15 = calcAdx(highs15, lows15, closes15, 14);
    const adxNow = adx15[adx15.length - 1];

    // Looser sideways filter
    if (adxNow != null && adxNow < Number(process.env.SCALP_MIN_ADX ?? 12)) {
      return { ok: false, reason: 'SCALP_LOW_ADX', details: { adxNow } };
    }

    const struct15 = structureLabel(candles15);
    const struct30 = structureLabel(candles30);

    // Don't take BUY into clear BEAR structure (and vice versa)
    if (bias === 'BUY' && (struct15 === 'BEAR' || struct30 === 'BEAR')) {
      return { ok: false, reason: 'SCALP_AGAINST_STRUCTURE', details: { struct15, struct30 } };
    }
    if (bias === 'SELL' && (struct15 === 'BULL' || struct30 === 'BULL')) {
      return { ok: false, reason: 'SCALP_AGAINST_STRUCTURE', details: { struct15, struct30 } };
    }

    // LTF confirmation (looser)
    if (c5?.rsi == null) return { ok: false, reason: 'NO_RSI_5M' };
    const buyMin = Number(process.env.SCALP_RSI5_BUY_MIN ?? 50.5);
    const sellMax = Number(process.env.SCALP_RSI5_SELL_MAX ?? 49.5);
    if (bias === 'BUY' && c5.rsi < buyMin) return { ok: false, reason: 'SCALP_NO_LTF_MOMENTUM' };
    if (bias === 'SELL' && c5.rsi > sellMax) return { ok: false, reason: 'SCALP_NO_LTF_MOMENTUM' };

    // Mild 15m RSI filter to avoid extremes
    const r15 = rsi15[rsi15.length - 1];
    if (r15 != null) {
      if (bias === 'BUY' && r15 > Number(process.env.SCALP_RSI15_BUY_MAX ?? 72)) return { ok: false, reason: 'SCALP_RSI15_TOO_HIGH', details: { r15 } };
      if (bias === 'SELL' && r15 < Number(process.env.SCALP_RSI15_SELL_MIN ?? 28)) return { ok: false, reason: 'SCALP_RSI15_TOO_LOW', details: { r15 } };
    }

    return { ok: true, mode: 'SCALP', struct15, struct30, adxNow };
  }

  const c30 = last(candles30);
  const c15 = last(candles15);
  const c5 = last(candles5);

  const closes30 = candles30.map(c => c.close);
  const closes15 = candles15.map(c => c.close);

  // recompute indicators for analysis robustness
  const ma50_30 = calcSma(closes30, 50);
  const ma50_15 = calcSma(closes15, 50);
  const rsi15 = calcRsi(closes15, 14);

  const highs15 = candles15.map(c => c.high);
  const lows15 = candles15.map(c => c.low);
  const adx15 = calcAdx(highs15, lows15, closes15, 14);

  const slope50 = maSlope(ma50_15, 10);
  const window = candles15.slice(Math.max(0, candles15.length - 40));
  const rangePct = window.length
    ? (Math.max(...window.map(c => c.high)) - Math.min(...window.map(c => c.low))) / c15.close
    : null;

  const sideways = isSideways({ adxNow: adx15[adx15.length - 1], slope50, rangePct });
  if (sideways) {
    return {
      ok: false,
      reason: 'SIDEWAYS_FILTER',
      details: { adx15: adx15[adx15.length - 1], slope50, rangePct },
    };
  }

  const struct30 = structureLabel(candles30);
  const struct15 = structureLabel(candles15);

  if (bias === 'BUY' && !(struct30 === 'BULL' || struct15 === 'BULL')) {
    return { ok: false, reason: 'STRUCTURE_NOT_BULL', details: { struct30, struct15 } };
  }
  if (bias === 'SELL' && !(struct30 === 'BEAR' || struct15 === 'BEAR')) {
    return { ok: false, reason: 'STRUCTURE_NOT_BEAR', details: { struct30, struct15 } };
  }

  // Divergence on 15m at MA zone
  const div = rsiDivergence({ candles: candles15, rsiArr: rsi15, type: bias === 'BUY' ? 'BULL' : 'BEAR' });
  if (!div.ok) {
    return { ok: false, reason: 'NO_RSI_DIVERGENCE' };
  }

  // Must be near MA zone to consider divergence actionable
  // Use DB-provided MA values on last 15m candle
  if (!inMaZone(c15, 0.006)) {
    return { ok: false, reason: 'NOT_AT_MA_ZONE' };
  }

  // LTF confirmation
  if (c5.rsi == null) return { ok: false, reason: 'NO_RSI_5M' };
  if (bias === 'BUY' && c5.rsi < 52) return { ok: false, reason: 'NO_LTF_MOMENTUM' };
  if (bias === 'SELL' && c5.rsi > 48) return { ok: false, reason: 'NO_LTF_MOMENTUM' };

  return {
    ok: true,
    struct30,
    struct15,
    div: {
      type: bias === 'BUY' ? 'BULL' : 'BEAR',
      priceA: div.a.price,
      priceB: div.b.price,
      rsiA: div.rA,
      rsiB: div.rB,
    },
    sidewaysDetails: { adx15: adx15[adx15.length - 1], slope50, rangePct },
  };
}

async function analyzeSymbol(symbol) {
  const data = {};
  for (const itv of INTERVALS) data[itv] = await getCandles(symbol, itv);

  const c5 = last(data['5m']);
  const c15 = last(data['15m']);
  const c30 = last(data['30m']);
  const c1h = last(data['1h']);
  const c4h = last(data['4h']);
  const c1d = last(data['1d']);

  const trend1d = trendLabel(c1d);
  const trend4h = trendLabel(c4h);
  const trend1h = trendLabel(c1h);

  const profile = (process.env.ANALYZE_PROFILE ?? 'strict').toLowerCase();

  let bias = decideBias(trend1d, trend4h, trend1h);

  // SCALP profile bias: prioritize 4H, then 1H.
  if (profile === 'scalp') {
    if (trend4h === 'BULL') bias = 'BUY';
    else if (trend4h === 'BEAR') bias = 'SELL';
    else if (trend1h === 'BULL') bias = 'BUY';
    else if (trend1h === 'BEAR') bias = 'SELL';
  }

  // TEMP DEBUG: If easy mode is on, allow a bias even when 2/3 HTFs are neutral,
  // so you can verify the pipeline creates trades.
  if (process.env.ANALYZE_EASY_MODE === '1' && bias === 'WAIT') {
    if (trend1d === 'BULL') bias = 'BUY';
    else if (trend1d === 'BEAR') bias = 'SELL';
  }

  let entryCheck = isEntrySignalV2({ bias, candles30: data['30m'], candles15: data['15m'], candles5: data['5m'] });

  const easyMode = process.env.ANALYZE_EASY_MODE === '1';

  // Compute candidate SL/TP (if we were to open)
  let setup = null;

  // SCALP profile: use fixed-risk SL/TP (more frequent than swing-based).
  if (!easyMode && profile === 'scalp' && bias !== 'WAIT' && entryCheck.ok) {
    const entry = Number(c5?.close);
    if (Number.isFinite(entry) && entry > 0) {
      const riskPct = Number(process.env.SCALP_RISK_PCT ?? 0.0015); // 0.15%
      const rMult = Number(process.env.SCALP_TP_R_MULT ?? 2); // TP = 2R (minimum RR 1:2)

      // Widen SL if market volatility is higher than pct-based risk (helps avoid too-tight stops)
      const highs5 = data['5m'].map(c => c.high);
      const lows5 = data['5m'].map(c => c.low);
      const closes5 = data['5m'].map(c => c.close);
      const atr5 = calcAtr(highs5, lows5, closes5, 14);
      const atrNow5 = atr5[atr5.length - 1];
      const slAtrMult = Number(process.env.SCALP_SL_ATR_MULT ?? 0); // set e.g. 0.8 to use ATR-based minimum risk
      const tpAtrMult = Number(process.env.SCALP_TP_ATR_MULT ?? 0); // set e.g. 1.6 to enforce minimum TP distance
      const minRiskPct = Number(process.env.SCALP_MIN_RISK_PCT ?? 0); // enforce minimum SL distance as % of entry
      const minTpPct = Number(process.env.SCALP_MIN_TP_PCT ?? 0); // enforce minimum TP distance as % of entry

      const riskAbsPct = entry * Math.max(0.0001, Math.min(riskPct, 0.02));
      const riskAbsMin = entry * Math.max(0, Math.min(minRiskPct, 0.05));
      const riskAbsAtr = atrNow5 != null ? atrNow5 * Math.max(0, slAtrMult) : 0;
      const riskAbs = Math.max(riskAbsPct, riskAbsMin, riskAbsAtr);

      const tpAbsR = rMult * riskAbs;
      const tpAbsMin = entry * Math.max(0, Math.min(minTpPct, 0.2));
      const tpAbsAtr = atrNow5 != null ? atrNow5 * Math.max(0, tpAtrMult) : 0;
      const tpAbs = Math.max(tpAbsR, tpAbsMin, tpAbsAtr);

      if (bias === 'BUY') {
        const sl = entry - riskAbs;
        const tp = entry + tpAbs;
        setup = {
          action: 'BUY',
          entry,
          sl,
          tp,
          rr: tpAbs / riskAbs,
          reasons: { entryCheck, sltpMeta: { atrNow5, riskAbs, tpAbs } },
        };
      } else {
        const sl = entry + riskAbs;
        const tp = entry - tpAbs;
        setup = {
          action: 'SELL',
          entry,
          sl,
          tp,
          rr: tpAbs / riskAbs,
          reasons: { entryCheck, sltpMeta: { atrNow5, riskAbs, tpAbs } },
        };
      }
    }
  }

  // EASY MODE: force a setup whenever bias is BUY/SELL so you can test the pipeline.
  // This bypasses entryCheck and swing-based SL/TP (which often fails due to missing indicators).
  if (easyMode && bias !== 'WAIT') {
    const base = Number(c5?.close);
    if (Number.isFinite(base) && base > 0) {
      const entry = base;
      const riskPct = Number(process.env.EASY_RISK_PCT ?? 0.003); // 0.3%
      const riskAbs = entry * Math.max(0.0001, Math.min(riskPct, 0.02));

      if (bias === 'BUY') {
        const sl = entry - riskAbs;
        const tp = entry + 2 * riskAbs;
        setup = {
          action: 'BUY',
          entry,
          sl,
          tp,
          rr: 2,
          reasons: { entryCheck: { ...entryCheck, ok: true, mode: 'EASY_FORCE' } },
        };
      } else {
        const sl = entry + riskAbs;
        const tp = entry - 2 * riskAbs;
        setup = {
          action: 'SELL',
          entry,
          sl,
          tp,
          rr: 2,
          reasons: { entryCheck: { ...entryCheck, ok: true, mode: 'EASY_FORCE' } },
        };
      }

      // Make it explicit in payload/logs
      entryCheck = { ...entryCheck, ok: true, mode: 'EASY_FORCE' };
    }
  }

  if (!setup && bias !== 'WAIT' && entryCheck.ok) {
    const base = c5.close;
    const offsetPctRaw = Number(process.env.ENTRY_OFFSET_PCT ?? 0.0005); // 0.05%
    const offsetMaxRaw = Number(process.env.ENTRY_OFFSET_MAX_PCT ?? 0.002); // 0.2%
    const offsetPct = Math.min(Math.max(offsetPctRaw, 0), Math.max(offsetMaxRaw, 0));
    const entry = bias === 'BUY'
      ? base * (1 - offsetPct)
      : base * (1 + offsetPct);
    // Swing-based SL/TP on 15m, but add volatility buffer so SL isn't unrealistically tight.
    // You can tune via env:
    // - SL_SWING_LOOKBACK (default 20)
    // - SL_ATR_MULT (default 0.35)  -> buffer beyond swing by ATR*mult
    // - MIN_RISK_PCT (default 0)    -> enforce minimum stop distance as % of entry
    const lookback = Number(process.env.SL_SWING_LOOKBACK ?? 20);
    const slAtrMult = Number(process.env.SL_ATR_MULT ?? 0.35);
    const tpAtrMult = Number(process.env.TP_ATR_MULT ?? 0);
    const minRiskPct = Number(process.env.MIN_RISK_PCT ?? 0);
    const minTpPct = Number(process.env.MIN_TP_PCT ?? 0);

    const highs15 = data['15m'].map(c => c.high);
    const lows15 = data['15m'].map(c => c.low);
    const closes15 = data['15m'].map(c => c.close);
    const atr15 = calcAtr(highs15, lows15, closes15, 14);
    const atrNow15 = atr15[atr15.length - 1];
    const slBuffer = atrNow15 != null ? atrNow15 * Math.max(0, slAtrMult) : 0;
    const tpMinAtr = atrNow15 != null ? atrNow15 * Math.max(0, tpAtrMult) : 0;
    const minRiskAbs = entry * Math.max(0, Math.min(minRiskPct, 0.05));
    const minTpAbs = entry * Math.max(0, Math.min(minTpPct, 0.2));

    if (bias === 'BUY') {
      const swing = recentSwingLow(data['15m'], lookback);
      // Wider (lower) SL = swing - ATR buffer, and also at least minRiskAbs away
      let sl = swing - slBuffer;
      if (minRiskAbs > 0) sl = Math.min(sl, entry - minRiskAbs);

      const risk = entry - sl;
      // Base TP = 2R, but enforce minimum TP distance using % and/or ATR
      const baseTp = entry + 2 * risk;
      const tp = Math.max(baseTp, entry + Math.max(minTpAbs, tpMinAtr));

      if (risk > 0) setup = {
        action: 'BUY',
        entry,
        sl,
        tp,
        rr: (tp - entry) / risk,
        reasons: { entryCheck, sltpMeta: { swing, slBuffer, atrNow15, lookback, minRiskPct, minTpPct, tpMinAtr } },
      };
    } else {
      const swing = recentSwingHigh(data['15m'], lookback);
      // Wider (higher) SL = swing + ATR buffer, and also at least minRiskAbs away
      let sl = swing + slBuffer;
      if (minRiskAbs > 0) sl = Math.max(sl, entry + minRiskAbs);

      const risk = sl - entry;
      // Base TP = 2R, but enforce minimum TP distance using % and/or ATR
      const baseTp = entry - 2 * risk;
      const tp = Math.min(baseTp, entry - Math.max(minTpAbs, tpMinAtr));

      if (risk > 0) setup = {
        action: 'SELL',
        entry,
        sl,
        tp,
        rr: (entry - tp) / risk,
        reasons: { entryCheck, sltpMeta: { swing, slBuffer, atrNow15, lookback, minRiskPct, minTpPct, tpMinAtr } },
      };
    }
  }

  const reentryReversal = reversalConfirmForContinuation({
    bias,
    candles15m: data['15m'],
    candles30m: data['30m'],
    candles1h: data['1h'],
  });

  return {
    symbol,
    snapshots: { c5, c15, c30, c1h, c4h, c1d },
    trends: { trend1d, trend4h, trend1h },
    bias,
    setup,
    reentryReversal,
  };
}

function parseSymbolQtyMap(raw) {
  // Format: "ETHUSDT:0.1,BTCUSDT:0.001"
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

async function main() {
  const runId = `analyzeAndAct:${Date.now()}`;
  const startedAt = Date.now();
  logger.info({ runId }, 'AnalyzeAndAct start');

  // If there's an open trade, focus on its symbol (strictly evaluate hold/close).
  const openTrades = await getActiveTrades();
  const focusSymbol = openTrades.length ? openTrades[0].symbol : null;

  // Default: analyze configured SYMBOLS from .env (config.symbols)
  let symbolsToAnalyze = focusSymbol ? [focusSymbol] : [...config.symbols];

  // If still empty, fallback to whatever exists in candles
  if (!symbolsToAnalyze.length) {
    const [rows] = await pool.query(`SELECT DISTINCT symbol FROM candles LIMIT 5`);
    symbolsToAnalyze = rows.map(r => r.symbol);
  }

  const qtyMap = parseSymbolQtyMap(process.env.SYMBOL_QTY_MAP);

  const results = [];
  for (const s of symbolsToAnalyze) results.push(await analyzeSymbol(s));

  const now = new Date().toISOString();

  // Action phase: manage existing trade (only the newest PENDING/ACTIVE)
  const actions = [];
  if (openTrades.length) {
    const t = openTrades[0];
    const r = results.find(x => x.symbol === t.symbol);
    const price = r?.snapshots?.c5?.close;

    // If we already have an order/position, we do NOT open a new one.
    // SL/TP fills are handled by orderWatcher (Binance order status).
    if (t.status === 'ACTIVE' && r) {
      const broke = structureBreak({ tradeSide: t.side, htf: r.snapshots.c4h });
      if (broke) {
        await cancelAndCloseTradeOnExchange(t, 'AUTO_CLOSE_HTF_BREAK', price);
        actions.push({ type: 'CLOSE', id: t.id, reason: 'HTF_BREAK', price });
      } else {
        actions.push({ type: 'HOLD', id: t.id, status: t.status, price });
      }
    } else {
      actions.push({ type: 'HOLD', id: t.id, status: t.status, note: 'Waiting for entry fill / watcher updates' });
    }
  }

  // If no open trade after management, consider opening new trade (strict)
  const openAfter = await getActiveTrades();
  if (!openAfter.length) {
    const r = results[0];
    const profile = (process.env.ANALYZE_PROFILE ?? 'strict').toLowerCase();
    const minRr = profile === 'scalp'
      ? Number(process.env.SCALP_MIN_RR ?? 2)
      : 2;

    // Re-entry safety: if the previous trade was stopped out while HTF trend is still strong,
    // require a reversal candle confirmation before opening again in the same HTF direction.
    const lastClosed = r?.symbol ? await getLastClosedTradeForSymbol(r.symbol) : null;

    if (r.setup && r.setup.rr >= minRr) {
      const blockInfo = shouldBlockReentryAfterStopout({
        lastClosedTrade: lastClosed,
        bias: r.bias,
        trends: r.trends,
        reversalConfirm: r.reentryReversal,
      });

      if (blockInfo.block) {
        actions.push({ type: 'WAIT', reason: blockInfo.reason, details: blockInfo.details });
      } else {
        // Quantity is user-defined in demo; use SYMBOL_QTY_MAP if available.
        const qty = qtyMap[r.symbol] ?? 0;
        if (!qty) {
          actions.push({ type: 'WAIT', reason: 'MISSING_QTY', hint: 'Set SYMBOL_QTY_MAP like ETHUSDT:0.1' });
        } else {
          const side = r.setup.action === 'BUY' ? 'LONG' : 'SHORT';
          await insertPendingTradeWithOrders({
            symbol: r.symbol,
            side,
            entry: format(r.setup.entry),
            sl: format(r.setup.sl),
            tp: format(r.setup.tp),
            qty: format(qty),
            leverage: null,
            notes: `AUTO_OPEN ${r.setup.action} rr=${r.setup.rr.toFixed(2)}`,
          });
          actions.push({ type: 'OPEN', symbol: r.symbol, side: r.setup.action, entry: r.setup.entry, sl: r.setup.sl, tp: r.setup.tp, rr: r.setup.rr, qty, status: 'PENDING' });
        }
      }
    } else {
      actions.push({ type: 'WAIT' });
    }
  }

  // Output as JSON to be displayed in chat
  const payload = {
    now,
    openTradesBefore: openTrades,
    analysis: results.map(r => ({
      symbol: r.symbol,
      trends: r.trends,
      bias: r.bias,
      reentryReversal: r.reentryReversal,
      setup: r.setup,
      snapshot: {
        '5m': r.snapshots.c5,
        '15m': r.snapshots.c15,
        '1h': r.snapshots.c1h,
        '4h': r.snapshots.c4h,
        '1d': r.snapshots.c1d,
      },
    })),
    actions,
  };

  // Summary log to file
  try {
    const opened = actions.filter(a => a.type === 'OPEN').length;
    const closed = actions.filter(a => a.type === 'CLOSE').length;
    const held = actions.filter(a => a.type === 'HOLD').length;
    const waited = actions.filter(a => a.type === 'WAIT').length;
    logger.info(
      {
        runId,
        symbolsAnalyzed: results.map(r => r.symbol),
        openTradesBefore: openTrades.map(t => ({ id: t.id, symbol: t.symbol, status: t.status, side: t.side })),
        actions,
        counts: { opened, closed, held, waited },
        ms: Date.now() - startedAt,
      },
      'AnalyzeAndAct done',
    );
  } catch (err) {
    logger.warn({ err, runId }, 'AnalyzeAndAct summary log failed');
  }

  // Use console.log so exec captures it
  console.log(JSON.stringify(payload, null, 2));

  // In long-lived scheduler mode, we must NOT close the shared pool.
  // But in one-shot mode (Task Scheduler / CLI), we DO want to close it so Node can exit.
  if (process.env.CLOSE_DB_POOL === '1') {
    await pool.end();
  }
}

try {
  // Top-level await so analyzeScheduler's import() waits for the run to finish.
  await main();
} catch (err) {
  logger.error({ err }, 'analyzeAndAct failed');
  // Let the error propagate to the scheduler's try/catch (import() will reject).
  throw err;
}
