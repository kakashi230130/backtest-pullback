import { adx as calcAdx, rsi as calcRsi, sma as calcSma, atr as calcAtr } from './indicators.js';

export const INTERVALS = ['5m', '15m', '30m', '1h', '4h', '1d'];

// Symbol-specific configuration (Requirement10)
export const DEFAULT_CONFIG = {
  adxThreshold: 20,         // ADX threshold to confirm trend
  atrMultiplier: 0.2,       // ATR multiplier for SL offset from swing
  maZoneAtp: 0.004,         // % distance to MA50 to consider "touch"
  rsiBuyRange: [50, 65],
  rsiSellRange: [35, 50],
  swingLookback: 5,
};

export const SYMBOL_CONFIGS = {
  BTCUSDT: { ...DEFAULT_CONFIG },
  ETHUSDT: {
    // ETH-specific optimization (Requirement9)
    adxThreshold: 25,
    atrMultiplier: 0.8,
    maZoneAtp: 0.001, // 0.1%
    rsiBuyRange: [52, 70],
    rsiSellRange: [30, 48],
    swingLookback: 10,
  },
};

function getSymbolConfig(symbol, symbolConfigsOverride = null) {
  const key = String(symbol ?? '').toUpperCase();
  const src = (symbolConfigsOverride && typeof symbolConfigsOverride === 'object') ? symbolConfigsOverride : null;
  return src?.[key] ?? SYMBOL_CONFIGS[key] ?? DEFAULT_CONFIG;
}

export function last(arr) {
  return arr?.length ? arr[arr.length - 1] : null;
}

export function trendLabel({ close, ma20, ma50, rsi }) {
  // Hysteresis around RSI=50 to reduce flicker during normal pullbacks.
  if (ma20 == null || ma50 == null || rsi == null || close == null) return 'NEUTRAL';
  if (close > ma50 && ma20 > ma50 && rsi >= 45) return 'BULL';
  if (close < ma50 && ma20 < ma50 && rsi <= 55) return 'BEAR';
  return 'NEUTRAL';
}

export function decideBias(t1, t2, t3) {
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

function isPullbackReversalCandle({ bias, candles15, candles30 }) {
  const m15 = candles15?.at(-1) ?? null;
  const m15p = candles15?.at(-2) ?? null;
  const m30 = candles30?.at(-1) ?? null;
  const m30p = candles30?.at(-2) ?? null;

  if (bias === 'BUY') {
    const bullEngulf = isBullishEngulfing(m15p, m15) || isBullishEngulfing(m30p, m30);
    const hammer = isHammerLike(m15) || isHammerLike(m30);
    return { ok: bullEngulf || hammer, why: bullEngulf ? 'BULL_ENGULF' : (hammer ? 'HAMMER' : 'NO_REVERSAL') };
  }
  if (bias === 'SELL') {
    const bearEngulf = isBearishEngulfing(m15p, m15) || isBearishEngulfing(m30p, m30);
    const star = isShootingStarLike(m15) || isShootingStarLike(m30);
    return { ok: bearEngulf || star, why: bearEngulf ? 'BEAR_ENGULF' : (star ? 'SHOOTING_STAR' : 'NO_REVERSAL') };
  }
  return { ok: false, why: 'BIAS_WAIT' };
}

function wickRejectionOk({ bias, c5, c15 }) {
  const need = Number(process.env.PULLBACK_WICK_MIN_PCT ?? 0.4);
  const minPct = Math.max(0.2, Math.min(need, 0.9));

  function okOne(c) {
    const x = candleParts(c);
    if (!x) return false;
    const range = x.high - x.low;
    if (!(range > 0)) return false;

    const lowerPct = x.lowerWick / range;
    const upperPct = x.upperWick / range;

    if (bias === 'BUY') {
      const requireBull = (process.env.PULLBACK_REQUIRE_BULL_REJECT ?? '0') === '1';
      if (requireBull && !x.isBull) return false;
      return lowerPct >= minPct;
    }

    if (bias === 'SELL') {
      const requireBear = (process.env.PULLBACK_REQUIRE_BEAR_REJECT ?? '0') === '1';
      if (requireBear && !x.isBear) return false;
      return upperPct >= minPct;
    }

    return false;
  }

  const ok = okOne(c5) || okOne(c15);
  return { ok, minPct };
}

function isEntrySignalStackedTrend({ bias, candles15, candles5, candles1h }) {
  // STACKED_TREND_STRATEGY (v4)
  if (bias !== 'BUY' && bias !== 'SELL') return { ok: false, reason: 'BIAS_WAIT' };

  const c15 = last(candles15);
  const c5 = last(candles5);
  if (!c15 || !c5) return { ok: false, reason: 'MISSING_CANDLES' };

  if (c15.ma20 == null || c15.ma50 == null || c15.ma200 == null) {
    return { ok: false, reason: 'MISSING_MA_15M' };
  }

  const poBuy = Number(c15.ma20) > Number(c15.ma50) && Number(c15.ma50) > Number(c15.ma200);
  const poSell = Number(c15.ma20) < Number(c15.ma50) && Number(c15.ma50) < Number(c15.ma200);

  if (bias === 'BUY' && !poBuy) return { ok: false, reason: 'PERFECT_ORDER_FAIL', details: { side: 'BUY' } };
  if (bias === 'SELL' && !poSell) return { ok: false, reason: 'PERFECT_ORDER_FAIL', details: { side: 'SELL' } };

  const slopeBars = Number(process.env.STACKED_MA200_SLOPE_BARS ?? 30);
  const n = Math.max(3, Math.min(slopeBars, 50));
  if (candles15.length < n + 1) return { ok: false, reason: 'MA200_SLOPE_NOT_ENOUGH_BARS' };
  const ma200Now = Number(candles15[candles15.length - 1].ma200);
  const ma200Prev = Number(candles15[candles15.length - 1 - n].ma200);
  if (!Number.isFinite(ma200Now) || !Number.isFinite(ma200Prev)) return { ok: false, reason: 'MA200_SLOPE_INVALID' };

  if (bias === 'BUY' && !(ma200Now > ma200Prev)) return { ok: false, reason: 'MA200_SLOPE_GUARD_FAIL', details: { side: 'BUY', ma200Now, ma200Prev, n } };
  if (bias === 'SELL' && !(ma200Now < ma200Prev)) return { ok: false, reason: 'MA200_SLOPE_GUARD_FAIL', details: { side: 'SELL', ma200Now, ma200Prev, n } };

  const adxMin1h = Number(process.env.STACKED_ADX1H_MIN ?? 20);
  if (adxMin1h > 0) {
    const arr1h = candles1h ?? [];
    if (arr1h.length < 40) return { ok: false, reason: 'ADX1H_NOT_ENOUGH_BARS' };
    const highs1h = arr1h.map(c => c.high);
    const lows1h = arr1h.map(c => c.low);
    const closes1h = arr1h.map(c => c.close);
    const adx1h = calcAdx(highs1h, lows1h, closes1h, 14);
    const adxNow1h = adx1h[adx1h.length - 1];
    if (adxNow1h == null || adxNow1h < adxMin1h) {
      return { ok: false, reason: 'ADX1H_TOO_LOW', details: { adxNow1h, adxMin1h } };
    }
  }

  const rsi15 = c15.rsi == null ? null : Number(c15.rsi);
  const rsi5 = c5.rsi == null ? null : Number(c5.rsi);
  if (!Number.isFinite(rsi15) || !Number.isFinite(rsi5)) return { ok: false, reason: 'NO_RSI' };

  const zoneLookback = Number(process.env.STACKED_RSI15_ZONE_LOOKBACK ?? 24);
  const lb = Math.max(4, Math.min(zoneLookback, 96));
  const rsi15Slice = candles15.slice(Math.max(0, candles15.length - lb)).map(x => (x.rsi == null ? null : Number(x.rsi))).filter(Number.isFinite);

  const buyDip = Number(process.env.STACKED_BUY_RSI15_DIP_BELOW ?? 42);
  const sellSpike = Number(process.env.STACKED_SELL_RSI15_SPIKE_ABOVE ?? 58);

  const hadBuyDip = rsi15Slice.some(v => v < buyDip);
  const hadSellSpike = rsi15Slice.some(v => v > sellSpike);

  const buyRsi5Min = Number(process.env.STACKED_BUY_RSI5_CONFIRM ?? 53);
  const sellRsi5Max = Number(process.env.STACKED_SELL_RSI5_CONFIRM ?? 47);

  if (bias === 'BUY') {
    if (!(rsi15 < 50 && rsi15 >= 35)) return { ok: false, reason: 'RSI15_NOT_IN_PULLBACK_ZONE', details: { rsi15 } };
    if (!hadBuyDip) return { ok: false, reason: 'RSI15_NO_DIP_BELOW_THRESHOLD', details: { buyDip, lookback: lb } };
    if (!(rsi5 > buyRsi5Min)) return { ok: false, reason: 'RSI5_NOT_RECLAIM_CONFIRM', details: { rsi5, buyRsi5Min } };
    return { ok: true, mode: 'STACKED_TREND', details: { rsi15, rsi5, ma200Now, ma200Prev, hadBuyDip, lb } };
  }

  if (!(rsi15 > 50 && rsi15 <= 65)) return { ok: false, reason: 'RSI15_NOT_IN_PULLBACK_ZONE', details: { rsi15 } };
  if (!hadSellSpike) return { ok: false, reason: 'RSI15_NO_SPIKE_ABOVE_THRESHOLD', details: { sellSpike, lookback: lb } };
  if (!(rsi5 < sellRsi5Max)) return { ok: false, reason: 'RSI5_NOT_DROP_CONFIRM', details: { rsi5, sellRsi5Max } };
  return { ok: true, mode: 'STACKED_TREND', details: { rsi15, rsi5, ma200Now, ma200Prev, hadSellSpike, lb } };
}

function isEntrySignalPullback({ bias, candles30, candles15, candles5 }) {
  if (bias !== 'BUY' && bias !== 'SELL') return { ok: false, reason: 'BIAS_WAIT' };

  const c15 = last(candles15);
  const c5 = last(candles5);
  if (!c15 || !c5) return { ok: false, reason: 'MISSING_CANDLES' };

  const highs15 = candles15.map(c => c.high);
  const lows15 = candles15.map(c => c.low);
  const closes15 = candles15.map(c => c.close);
  const adx15 = calcAdx(highs15, lows15, closes15, 14);
  const adxNow = adx15[adx15.length - 1];
  const minAdx = Number(process.env.PULLBACK_ADX_MIN ?? 25);
  if (adxNow == null || adxNow < minAdx) {
    return { ok: false, reason: 'PULLBACK_ADX_TOO_LOW', details: { adxNow, minAdx } };
  }

  const zone = Number(process.env.MA_ZONE_PCT ?? 0.01);
  const maZonePct = Math.max(0.006, Math.min(zone, 0.02));
  if (!inMaZone(c15, maZonePct)) return { ok: false, reason: 'NOT_AT_MA_ZONE', details: { maZonePct } };

  const wick = wickRejectionOk({ bias, c5, c15 });
  if (!wick.ok) return { ok: false, reason: 'NO_WICK_REJECTION', details: { minPct: wick.minPct } };

  const requirePattern = (process.env.PULLBACK_REQUIRE_PATTERN ?? '1') === '1';
  const rev = isPullbackReversalCandle({ bias, candles15, candles30 });
  if (requirePattern && !rev.ok) return { ok: false, reason: 'NO_REVERSAL_CANDLE', details: rev };

  if (c5?.rsi == null) return { ok: false, reason: 'NO_RSI_5M' };
  const buyMin = Number(process.env.PULLBACK_RSI5_BUY_MIN ?? 45);
  const sellMax = Number(process.env.PULLBACK_RSI5_SELL_MAX ?? 55);
  if (bias === 'BUY' && c5.rsi < buyMin) return { ok: false, reason: 'NO_LTF_MOMENTUM', details: { rsi5: c5.rsi, buyMin } };
  if (bias === 'SELL' && c5.rsi > sellMax) return { ok: false, reason: 'NO_LTF_MOMENTUM', details: { rsi5: c5.rsi, sellMax } };

  const lookback = Number(process.env.VOLUME_LOOKBACK ?? 20);
  const mult = Number(process.env.VOLUME_MULT ?? 1.5);
  const tf = String(process.env.PULLBACK_VOLUME_TF ?? '5m').toLowerCase();

  const v5 = volumeSpikeOk(candles5, { lookback, mult });
  const v15 = volumeSpikeOk(candles15, { lookback, mult });

  if (tf === '5m' && !v5.ok) return { ok: false, reason: 'VOLUME_FILTER_5M', details: v5 };
  if (tf === '15m' && !v15.ok) return { ok: false, reason: 'VOLUME_FILTER_15M', details: v15 };
  if (tf === 'both' && (!v5.ok || !v15.ok)) return { ok: false, reason: 'VOLUME_FILTER_BOTH', details: { v5, v15 } };

  return { ok: true, mode: 'PULLBACK' };
}

function maSlope(maArr, bars = 10) {
  const n = maArr.length;
  if (n < bars + 1) return null;
  const a = maArr[n - 1];
  const b = maArr[n - 1 - bars];
  if (a == null || b == null || b === 0) return null;
  return (a - b) / b;
}

function isSideways({ adxNow, slope50, rangePct }) {
  if (adxNow != null && adxNow < 18) return true;
  if (slope50 != null && Math.abs(slope50) < 0.002) return true;
  if (rangePct != null && rangePct < 0.006) return true;
  return false;
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
  if (x.body <= 0) return false;
  return x.upperWick >= 2.2 * x.body && x.lowerWick <= 0.8 * x.body;
}

function isHammerLike(c) {
  const x = candleParts(c);
  if (!x) return false;
  if (x.body <= 0) return false;
  return x.lowerWick >= 2.2 * x.body && x.upperWick <= 0.8 * x.body;
}

function volumeSpikeOk(candles, { lookback = 20, mult = 1.5 } = {}) {
  if (!candles || candles.length < lookback + 2) return { ok: false, reason: 'VOLUME_NOT_ENOUGH_BARS' };

  const useMult = Math.max(1.0, Math.min(Number(mult) || 1.5, 10));
  const n = Math.max(5, Math.min(Number(lookback) || 20, 200));

  const cur = candles[candles.length - 1];
  const prev = candles.slice(Math.max(0, candles.length - 1 - n), candles.length - 1);

  const curVol = Number(cur?.volume);
  if (!Number.isFinite(curVol)) return { ok: false, reason: 'VOLUME_CUR_INVALID' };

  const vols = prev.map(x => Number(x.volume)).filter(Number.isFinite);
  if (vols.length < Math.max(5, Math.floor(n * 0.6))) return { ok: false, reason: 'VOLUME_PREV_INVALID' };

  const avg = vols.reduce((a, b) => a + b, 0) / vols.length;
  if (!(avg > 0)) return { ok: false, reason: 'VOLUME_AVG_INVALID' };

  const ok = curVol >= avg * useMult;
  return { ok, curVol, avg, mult: useMult, lookback: n };
}

function checkCandleConfirm15m(candles15m, direction) {
  const slice = candles15m.slice(-4);
  if (slice.length < 2) return { ok: false, reason: 'NOT_ENOUGH_CANDLES' };

  const c0 = slice.at(-1);
  const c1 = slice.at(-2);
  const c2 = slice.at(-3);

  if (direction === 'BUY') {
    if (isBullishEngulfing(c1, c0)) return { ok: true, pattern: 'BULL_ENGULF', tf: '15m', candle: 'last' };
    if (c2 && isBullishEngulfing(c2, c1)) return { ok: true, pattern: 'BULL_ENGULF', tf: '15m', candle: 'prev' };
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

function volumeOk15m(candles, mult = 1.3, lookback = 20) {
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

function isEntrySignalPullbackToMa({ bias, candles15, candles1h, candles4h, candles1d, btcContext = null, requireBtcContext = false }) {
  if (bias !== 'BUY' && bias !== 'SELL') return { ok: false, reason: 'BIAS_WAIT' };

  const maZonePct = Number(process.env.PULLBACK_MA_ZONE_PCT ?? 0.004);

  // RSI 1H filter (Requirement):
  // - BUY only when RSI(14) on 1H is in [50, 65]
  // - SELL only when RSI(14) on 1H is in [35, 50]
  // Optional env overrides (easy tuning):
  // - RSI_BUY_LOW / RSI_BUY_HIGH
  // - RSI_SELL_LOW / RSI_SELL_HIGH
  const rsiBuyLow = Number(process.env.RSI_BUY_LOW ?? 50);
  const rsiBuyHigh = Number(process.env.RSI_BUY_HIGH ?? 65);
  const rsiSellLow = Number(process.env.RSI_SELL_LOW ?? 35);
  const rsiSellHigh = Number(process.env.RSI_SELL_HIGH ?? 50);

  const volMult = Number(process.env.PULLBACK_VOLUME_MULT ?? 1.3);
  const minRr = Number(process.env.PULLBACK_MIN_RR ?? 1.8);
  const requireConfirm = (process.env.PULLBACK_REQUIRE_CONFIRM ?? '1') === '1';
  const useMA200 = (process.env.PULLBACK_USE_MA200 ?? '1') === '1';

  const last1h = last(candles1h);
  const last15 = last(candles15);
  if (!last1h || !last15) return { ok: false, reason: 'MISSING_TF' };

  // Step 2: pullback near MA20/MA50/(MA200) on 1H
  const close1h = last1h.close;
  const ma20_1h = last1h.ma20;
  const ma50_1h = last1h.ma50;
  const ma200_1h = last1h.ma200;

  const dMA20 = ma20_1h != null ? pctDist(close1h, ma20_1h) : null;
  const dMA50 = ma50_1h != null ? pctDist(close1h, ma50_1h) : null;
  const dMA200 = ma200_1h != null ? pctDist(close1h, ma200_1h) : null;

  const nearMA20 = dMA20 != null && dMA20 <= maZonePct;
  const nearMA50 = dMA50 != null && dMA50 <= maZonePct;
  const nearMA200 = useMA200 && dMA200 != null && dMA200 <= maZonePct;

  if (!(nearMA20 || nearMA50 || nearMA200)) {
    return { ok: false, reason: 'NOT_AT_MA_ZONE', details: { dMA20, dMA50, dMA200, maZonePct } };
  }

  const whichMA = nearMA200 ? 'MA200' : (nearMA50 ? 'MA50' : 'MA20');

  // Step 2b: anti-reversal guard around MA50
  if (bias === 'BUY' && ma50_1h != null && close1h < ma50_1h * 0.995) {
    return { ok: false, reason: 'BULL_PRICE_BELOW_MA50' };
  }
  if (bias === 'SELL' && ma50_1h != null && close1h > ma50_1h * 1.005) {
    return { ok: false, reason: 'BEAR_PRICE_ABOVE_MA50' };
  }

  // Step 2c: BTC market regime / correlation filter (Requirement v2)
  // Enforce BTC 1H regime alignment for alt entries.
  // - If BTC candle is missing (when required): hard fail to avoid silent wrong backtests.
  // - Long Alt:  BTC RSI(1H) > 45 AND BTC Close > BTC MA50(1H)
  // - Short Alt: BTC RSI(1H) < 55 AND BTC Close < BTC MA50(1H)
  if (requireBtcContext && !btcContext) {
    return { ok: false, reason: 'btc_data_missing', debug: { btc_close: null, btc_ma50: null, btc_rsi: null } };
  }

  if (btcContext) {
    const btcClose = Number(btcContext.close);
    const btcMa50 = Number(btcContext.ma50);
    const btcRsi = Number(btcContext.rsi);

    const btcIsBull = btcClose > btcMa50 && btcRsi > 50;
    const btcIsBear = btcClose < btcMa50 && btcRsi < 50;

    if (bias === 'BUY' && !btcIsBull) return { ok: false, reason: 'btc_not_bullish' };
    if (bias === 'SELL' && !btcIsBear) return { ok: false, reason: 'btc_not_bearish' };

    const debugBtc = { btc_close: btcClose, btc_ma50: btcMa50, btc_rsi: btcRsi };

    if (![btcClose, btcMa50, btcRsi].every(Number.isFinite)) {
      return { ok: false, reason: 'btc_correlation_fail', debug: debugBtc };
    }

    const ok = bias === 'BUY'
      ? (btcRsi > 45 && btcClose > btcMa50)
      : (btcRsi < 55 && btcClose < btcMa50);

    if (!ok) {
      return { ok: false, reason: 'btc_correlation_fail', debug: debugBtc };
    }
  }

  // Step 3: ADX 1H filter (Requirement)
  // Only trade when trend strength is clear: ADX(14) on 1H must be >= threshold.
  const adxMin = Number(process.env.ADX_MIN_LEVEL ?? 25);
  const highs1h = candles1h.map(c => c.high);
  const lows1h = candles1h.map(c => c.low);
  const closes1h = candles1h.map(c => c.close);
  const adxArr1h = calcAdx(highs1h, lows1h, closes1h, 14);
  const adx1h = adxArr1h[adxArr1h.length - 1];

  const btcDebug = btcContext
    ? { btc_close: Number(btcContext.close), btc_ma50: Number(btcContext.ma50), btc_rsi: Number(btcContext.rsi) }
    : (requireBtcContext ? { btc_close: null, btc_ma50: null, btc_rsi: null } : null);

  if (adx1h == null) return { ok: false, reason: 'NO_ADX_1H', debug: { adx1h: null, ...(btcDebug ?? {}) } };
  if (adx1h < adxMin) {
    return { ok: false, reason: 'WEAK_TREND_ADX', debug: { adx1h, adxMin, ...(btcDebug ?? {}) } };
  }

  // Step 4: RSI 1H zone (recomputed from closes)
  const rsiArr1h = calcRsi(closes1h, 14);
  const rsi1h = rsiArr1h[rsiArr1h.length - 1];
  if (rsi1h == null) return { ok: false, reason: 'NO_RSI_1H', debug: { rsi1h: null, adx1h, ...(btcDebug ?? {}) } };

  let rsiOk = false;
  if (bias === 'BUY' && rsi1h >= rsiBuyLow && rsi1h <= rsiBuyHigh) rsiOk = true;
  if (bias === 'SELL' && rsi1h >= rsiSellLow && rsi1h <= rsiSellHigh) rsiOk = true;
  if (!rsiOk) {
    return {
      ok: false,
      reason: 'RSI_OUT_OF_ZONE',
      details: { rsi1h, bias, rsiBuyLow, rsiBuyHigh, rsiSellLow, rsiSellHigh },
      debug: { rsi1h, adx1h, ...(btcDebug ?? {}) },
    };
  }

  // Step 4: confirmation candle on 15m
  let confirmResult = { ok: true, pattern: 'SKIPPED' };
  if (requireConfirm) {
    confirmResult = checkCandleConfirm15m(candles15, bias);
    if (!confirmResult.ok) return { ok: false, reason: confirmResult.reason, details: { confirmResult, bias } };
  }

  // Step 5: volume spike on 15m
  const volResult = volumeOk15m(candles15, volMult, 20);
  if (!volResult.ok) return { ok: false, reason: 'VOLUME_FILTER', details: volResult };

  // RR filter is enforced in SLTP builder; keep minRr for reporting
  return {
    ok: true,
    mode: 'PULLBACK_TO_MA',
    details: { whichMA, rsi1h, confirmResult, volResult, minRr },
    // Requirement: keep RSI in debug so it shows up in backtest result logs.
    debug: { rsi1h, adx1h, ...(btcDebug ?? {}) },
  };
}

function buildSltpPullbackToMa({ bias, entry, candles1h, atrMultiplier = null, swingLookback = 5 }) {
  // New SL/TP logic (per requirement):
  // - Use swing low/high of last 5 candles on 1H
  // - SL = swing +/- 0.2*ATR(1H)
  // - If SL invalid vs entry, force SL = entry +/- 1.5*ATR(1H)
  // - TP maintains RR=1:2 => TP = entry +/- dist*2
  const minRr = Number(process.env.PULLBACK_MIN_RR ?? 1.8);

  if (!candles1h || candles1h.length < 20) return { ok: false, reason: 'NOT_ENOUGH_1H' };

  const highs1h = candles1h.map(c => c.high);
  const lows1h = candles1h.map(c => c.low);
  const closes1h = candles1h.map(c => c.close);
  const atrArr1h = calcAtr(highs1h, lows1h, closes1h, 14);
  const atr1h = atrArr1h[atrArr1h.length - 1];
  if (atr1h == null || !(atr1h > 0)) return { ok: false, reason: 'NO_ATR_1H' };

  const nSwing = Math.max(3, Math.min(Number(swingLookback) || 5, 20));
  const swingSlice = candles1h.slice(-nSwing);
  if (swingSlice.length < nSwing) return { ok: false, reason: 'NOT_ENOUGH_SWING_BARS' };

  const swingLow = Math.min(...swingSlice.map(c => Number(c.low)).filter(Number.isFinite));
  const swingHigh = Math.max(...swingSlice.map(c => Number(c.high)).filter(Number.isFinite));
  if (!Number.isFinite(swingLow) || !Number.isFinite(swingHigh)) return { ok: false, reason: 'SWING_INVALID' };

  // Replace hardcoded 0.2 ATR offset with config.atrMultiplier (Requirement10)
  const mult = atrMultiplier == null ? DEFAULT_CONFIG.atrMultiplier : Number(atrMultiplier);
  const useMult = Number.isFinite(mult) ? Math.max(0.01, Math.min(mult, 10)) : DEFAULT_CONFIG.atrMultiplier;
  const trail = useMult * atr1h;

  // Keep the legacy force fallback for safety
  const force = 1.5 * atr1h;

  let sl, tp, dist;

  if (bias === 'BUY') {
    sl = swingLow - trail;
    if (!(sl < entry)) sl = entry - force;
    dist = entry - sl;
    if (!(dist > 0)) return { ok: false, reason: 'INVALID_RISK_BUY', entry, sl };
    tp = entry + dist * 2;
  } else {
    sl = swingHigh + trail;
    if (!(sl > entry)) sl = entry + force;
    dist = sl - entry;
    if (!(dist > 0)) return { ok: false, reason: 'INVALID_RISK_SELL', entry, sl };
    tp = entry - dist * 2;
  }

  const rr = 2;
  if (rr < minRr) return { ok: false, reason: 'RR_TOO_LOW', rr, minRr };

  return {
    ok: true,
    sl,
    tp,
    rr,
    meta: {
      strategy: 'PULLBACK_TO_MA',
      atr1h,
      swingLow,
      swingHigh,
      swingBars: nSwing,
      slTrailAtr: useMult,
      slForceAtr: 1.5,
    },
  };
}

export function isEntrySignalV2({ bias, candles30, candles15, candles5, candles1h = null }) {
  const profile = (process.env.ANALYZE_PROFILE ?? 'strict').toLowerCase();
  const strategy = String(process.env.ANALYZE_STRATEGY ?? 'DEFAULT').toUpperCase();

  if (strategy === 'PULLBACK_STRATEGY') {
    return isEntrySignalPullback({ bias, candles30, candles15, candles5 });
  }

  if (strategy === 'STACKED_TREND_STRATEGY') {
    return isEntrySignalStackedTrend({ bias, candles15, candles5, candles1h: candles1h ?? [] });
  }

  if (bias !== 'BUY' && bias !== 'SELL') return { ok: false, reason: 'BIAS_WAIT' };

  const c15 = last(candles15);
  const c5 = last(candles5);

  const closes30 = candles30.map(c => c.close);
  const closes15 = candles15.map(c => c.close);

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

  // fallback behavior (strict legacy) omitted in minimal focus
  return { ok: false, reason: 'STRICT_DISABLED' };
}

function recentSwingLow(candles, lookback = 20) {
  const slice = candles.slice(Math.max(0, candles.length - lookback));
  return Math.min(...slice.map(c => c.low));
}

function recentSwingHigh(candles, lookback = 20) {
  const slice = candles.slice(Math.max(0, candles.length - lookback));
  return Math.max(...slice.map(c => c.high));
}

// analyzeSymbolFromCandles supports both:
// - legacy object signature: ({ symbol, data, nowMs, btcContext, requireBtcContext, symbolConfigs })
// - requirement10 positional signature:
//   (symbol, candles5, candles15, candles1h, snapData, btcContext, requireBtcContext, nowMs, symbolConfigs)
export function analyzeSymbolFromCandles(...args) {
  let symbol;
  let data;
  let nowMs;
  let btcContext = null;
  let requireBtcContext = false;
  let symbolConfigs = null;

  if (args.length === 1 && args[0] && typeof args[0] === 'object' && 'data' in args[0]) {
    ({ symbol, data, nowMs, btcContext = null, requireBtcContext = false, symbolConfigs = null } = args[0]);
  } else {
    // positional
    [symbol, , , , data, btcContext = null, requireBtcContext = false, nowMs, symbolConfigs = null] = args;
  }

  const c5 = last(data['5m']);

  const maxStalenessMs = Number(process.env.MAX_CANDLE_STALENESS_MS ?? 15 * 60 * 1000);
  if (c5?.open_time && maxStalenessMs > 0 && nowMs != null) {
    const ageMs = Number(nowMs) - Number(c5.open_time);
    if (Number.isFinite(ageMs) && ageMs > maxStalenessMs) {
      return null;
    }
  }

  const c15 = last(data['15m']);
  const c1h = last(data['1h']);
  const c4h = last(data['4h']);
  const c1d = last(data['1d']);

  // Pullback-to-MA strategy (ported from analyzePullback.js)
  const strategy = String(process.env.ANALYZE_STRATEGY ?? 'PULLBACK_TO_MA').toUpperCase();

  // Trend filter on HTF: use MA20/MA50 agreement on 4H + 1D
  function trendOf(c) {
    if (!c || c.ma20 == null || c.ma50 == null || c.close == null) return 'NEUTRAL';
    if (c.close > c.ma50 && c.ma20 > c.ma50) return 'BULL';
    if (c.close < c.ma50 && c.ma20 < c.ma50) return 'BEAR';
    return 'NEUTRAL';
  }

  const trend4h = trendOf(c4h);
  const trend1d = trendOf(c1d);
  let htfTrend = 'NEUTRAL';
  if (trend4h === 'BULL' && trend1d === 'BULL') htfTrend = 'BULL';
  else if (trend4h === 'BEAR' && trend1d === 'BEAR') htfTrend = 'BEAR';

  let bias = htfTrend === 'BULL' ? 'BUY' : (htfTrend === 'BEAR' ? 'SELL' : 'WAIT');

  // ADX regime filter (Requirement10): only allow BUY/SELL bias when ADX(14) on 1H >= config.adxThreshold.
  const cfg = getSymbolConfig(symbol, symbolConfigs);
  const adxMinBias = Number(cfg?.adxThreshold ?? DEFAULT_CONFIG.adxThreshold);

  const arr1h = data['1h'] ?? [];
  if (bias !== 'WAIT') {
    if (arr1h.length < 40) {
      bias = 'WAIT';
    } else {
      const highs1h = arr1h.map(c => c.high);
      const lows1h = arr1h.map(c => c.low);
      const closes1h = arr1h.map(c => c.close);
      const adx1hArr = calcAdx(highs1h, lows1h, closes1h, 14);
      const adx1hNow = adx1hArr[adx1hArr.length - 1];
      if (!(adx1hNow != null && adx1hNow >= adxMinBias)) {
        bias = 'WAIT';
      }
    }
  }

  // Entry check
  let entryCheck = { ok: false, reason: 'BIAS_WAIT' };
  let setup = null;

  // If HTF trend says BUY/SELL but bias got nulled by ADX regime, mark explicit reason.
  if (htfTrend !== 'NEUTRAL' && bias === 'WAIT') {
    entryCheck = { ok: false, reason: 'WEAK_TREND_ADX' };
  }

  if (strategy === 'PULLBACK_TO_MA') {
    // Only run the expensive entry checks if bias is BUY/SELL.
    if (bias === 'WAIT') {
      // keep the reason as-is (BIAS_WAIT or WEAK_TREND_ADX)
    } else {
      entryCheck = isEntrySignalPullbackToMa({
        bias,
        candles15: data['15m'],
        candles1h: data['1h'],
        candles4h: data['4h'],
        candles1d: data['1d'],
        btcContext,
        requireBtcContext,
        symbol,
        symbolConfigs,
      });
    }

    if (bias !== 'WAIT' && entryCheck.ok) {
      const entry = c15?.close;
      if (entry != null) {
        const cfg = getSymbolConfig(symbol, symbolConfigs);
        const atrMultiplier = cfg?.atrMultiplier ?? DEFAULT_CONFIG.atrMultiplier;
        const swingLookback = cfg?.swingLookback ?? DEFAULT_CONFIG.swingLookback;

        // ETH-specific 5m impulse confirmation (Requirement9)
        // Confirm the 5m candle body is at least +15% vs previous candle (momentum back to trend).
        if (String(symbol).toUpperCase() === 'ETHUSDT') {
          const c5Now = last(data['5m']);
          const c5Prev = data['5m']?.at(-2) ?? null;
          const pNow = candleParts(c5Now);
          const pPrev = candleParts(c5Prev);
          if (!pNow || !pPrev || !(pPrev.body > 0)) {
            entryCheck = { ok: false, reason: 'WEAK_CONFIRM_CANDLE', details: { tf: '5m', why: 'NO_PREV_BODY' } };
          } else {
            const need = pPrev.body * 1.15;
            const dirOk = bias === 'BUY' ? (pNow.close > pNow.open) : (pNow.close < pNow.open);
            if (!dirOk || !(pNow.body >= need)) {
              entryCheck = { ok: false, reason: 'WEAK_CONFIRM_CANDLE', details: { tf: '5m', dir: bias, curBody: pNow.body, prevBody: pPrev.body, need } };
            }
          }
        }

        if (entryCheck.ok) {
          const sltp = buildSltpPullbackToMa({ bias, entry, candles1h: data['1h'], atrMultiplier, swingLookback });
          if (sltp?.ok) {
            setup = {
              action: bias,
              entry,
              sl: sltp.sl,
              tp: sltp.tp,
              rr: sltp.rr,
              reasons: { entryCheck, sltpMeta: sltp.meta },
            };
          } else {
            entryCheck = { ok: false, reason: sltp?.reason ?? 'SLTP_FAIL', details: sltp ?? null };
          }
        }
      } else {
        entryCheck = { ok: false, reason: 'MISSING_15M' };
      }
    }
  } else {
    // fallback: keep minimal behavior (no setups)
    entryCheck = { ok: false, reason: 'UNKNOWN_STRATEGY', details: { strategy } };
  }

  return {
    symbol,
    snapshots: { c5, c15, c1h, c4h, c1d },
    trends: { trend4h, trend1d, htfTrend },
    bias,
    entryCheck,
    setup,
  };
}
