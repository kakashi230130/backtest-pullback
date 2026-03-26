import { adx as calcAdx, rsi as calcRsi, sma as calcSma, atr as calcAtr } from './indicators.js';

export const INTERVALS = ['5m', '15m', '30m', '1h', '4h', '1d'];

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

export function analyzeSymbolFromCandles({ symbol, data, nowMs }) {
  const c5 = last(data['5m']);

  const maxStalenessMs = Number(process.env.MAX_CANDLE_STALENESS_MS ?? 15 * 60 * 1000);
  if (c5?.open_time && maxStalenessMs > 0 && nowMs != null) {
    const ageMs = Number(nowMs) - Number(c5.open_time);
    if (Number.isFinite(ageMs) && ageMs > maxStalenessMs) {
      return null;
    }
  }

  const c30 = last(data['30m']);
  const c1h = last(data['1h']);
  const c4h = last(data['4h']);
  const c1d = last(data['1d']);

  const trend1d = trendLabel(c1d ?? {});
  const trend4h = trendLabel(c4h ?? {});
  const trend1h = trendLabel(c1h ?? {});

  let bias = decideBias(trend1d, trend4h, trend1h);
  if (bias === 'BUY') bias = 'WAIT';
  let entryCheck = isEntrySignalV2({
    bias,
    candles30: data['30m'],
    candles15: data['15m'],
    candles5: data['5m'],
    candles1h: data['1h'],
  });

  let setup = null;

  if (!setup && bias !== 'WAIT' && entryCheck.ok) {
    const base = c5.close;
    const offsetPctRaw = Number(process.env.ENTRY_OFFSET_PCT ?? 0.0005);
    const offsetMaxRaw = Number(process.env.ENTRY_OFFSET_MAX_PCT ?? 0.002);
    const offsetPct = Math.min(Math.max(offsetPctRaw, 0), Math.max(offsetMaxRaw, 0));
    const entry = bias === 'BUY' ? base * (1 - offsetPct) : base * (1 + offsetPct);

    const strategy = String(process.env.ANALYZE_STRATEGY ?? 'DEFAULT').toUpperCase();

    if (strategy === 'STACKED_TREND_STRATEGY') {
      const highs15 = data['15m'].map(c => c.high);
      const lows15 = data['15m'].map(c => c.low);
      const closes15 = data['15m'].map(c => c.close);
      const atr15 = calcAtr(highs15, lows15, closes15, 14);
      const atrNow15 = atr15[atr15.length - 1];
      const k = Number(process.env.STACKED_SL_ATR_MULT ?? 2.0);
      const slDist = atrNow15 != null ? atrNow15 * Math.max(0, k) : null;
      if (slDist != null && slDist > 0) {
        const rr = Number(process.env.STACKED_TP_RR ?? 2.0);
        const rrMult = Math.max(0.5, Math.min(rr, 5));
        if (bias === 'BUY') {
          const sl = entry - slDist;
          const risk = entry - sl;
          const tp = entry + rrMult * risk;
          if (risk > 0) setup = { action: 'BUY', entry, sl, tp, rr: rrMult, reasons: { entryCheck, sltpMeta: { strategy, atrNow15, slDist, rrMult } } };
        } else {
          const sl = entry + slDist;
          const risk = sl - entry;
          const tp = entry - rrMult * risk;
          if (risk > 0) setup = { action: 'SELL', entry, sl, tp, rr: rrMult, reasons: { entryCheck, sltpMeta: { strategy, atrNow15, slDist, rrMult } } };
        }
      }
    } else {
      // minimal: no other SLTP
      setup = null;
    }
  }

  return {
    symbol,
    snapshots: { c5, c30, c1h, c4h, c1d },
    trends: { trend1d, trend4h, trend1h },
    bias,
    entryCheck,
    setup,
  };
}
