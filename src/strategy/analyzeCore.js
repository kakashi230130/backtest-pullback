import { adx as calcAdx, rsi as calcRsi, sma as calcSma, atr as calcAtr } from '../indicators.js';

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

export function reversalConfirmForContinuation({ bias, candles15m, candles30m, candles1h }) {
  const m15c = candles15m?.slice(-4) ?? [];
  const m30c = candles30m?.slice(-4) ?? [];
  const h1c = candles1h?.slice(-4) ?? [];

  const m15 = m15c.at(-1) ?? null;
  const m15Prev = m15c.at(-2) ?? null;
  const m15Prev2 = m15c.at(-3) ?? null;

  const m30 = m30c.at(-1) ?? null;
  const m30Prev = m30c.at(-2) ?? null;
  const m30Prev2 = m30c.at(-3) ?? null;

  const h1 = h1c.at(-1) ?? null;
  const h1Prev = h1c.at(-2) ?? null;
  const h1Prev2 = h1c.at(-3) ?? null;

  if (bias === 'SELL') {
    const bearEngulf =
      isBearishEngulfing(m15Prev2, m15Prev) ||
      isBearishEngulfing(m15Prev, m15) ||
      isBearishEngulfing(m30Prev2, m30Prev) ||
      isBearishEngulfing(m30Prev, m30) ||
      isBearishEngulfing(h1Prev2, h1Prev) ||
      isBearishEngulfing(h1Prev, h1);

    const rejectUp = isShootingStarLike(m15) || isShootingStarLike(m30) || isShootingStarLike(h1);

    return {
      ok: bearEngulf || rejectUp,
      why: bearEngulf ? 'BEAR_ENGULF' : (rejectUp ? 'UPPER_WICK_REJECT' : 'NO_REVERSAL'),
      tf: bearEngulf
        ? (
          (isBearishEngulfing(m15Prev2, m15Prev) || isBearishEngulfing(m15Prev, m15))
            ? '15m'
            : ((isBearishEngulfing(m30Prev2, m30Prev) || isBearishEngulfing(m30Prev, m30)) ? '30m' : '1h')
        )
        : (rejectUp ? (isShootingStarLike(m15) ? '15m' : (isShootingStarLike(m30) ? '30m' : '1h')) : null),
    };
  }

  if (bias === 'BUY') {
    const bullEngulf =
      isBullishEngulfing(m15Prev2, m15Prev) ||
      isBullishEngulfing(m15Prev, m15) ||
      isBullishEngulfing(m30Prev2, m30Prev) ||
      isBullishEngulfing(m30Prev, m30) ||
      isBullishEngulfing(h1Prev2, h1Prev) ||
      isBullishEngulfing(h1Prev, h1);

    const rejectDown = isHammerLike(m15) || isHammerLike(m30) || isHammerLike(h1);

    return {
      ok: bullEngulf || rejectDown,
      why: bullEngulf ? 'BULL_ENGULF' : (rejectDown ? 'LOWER_WICK_REJECT' : 'NO_REVERSAL'),
      tf: bullEngulf
        ? (
          (isBullishEngulfing(m15Prev2, m15Prev) || isBullishEngulfing(m15Prev, m15))
            ? '15m'
            : ((isBullishEngulfing(m30Prev2, m30Prev) || isBullishEngulfing(m30Prev, m30)) ? '30m' : '1h')
        )
        : (rejectDown ? (isHammerLike(m15) ? '15m' : (isHammerLike(m30) ? '30m' : '1h')) : null),
    };
  }

  return { ok: false, why: 'BIAS_WAIT', tf: null };
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
  const { highs, lows } = swingPoints(candles);
  const minSwingGap = Math.max(1, Math.min(Number(process.env.DIV_MIN_SWING_GAP ?? 5) || 5, 50));

  if (type === 'BULL') {
    if (lows.length < 2) return { ok: false };
    const a = lows[lows.length - 2];
    const b = lows[lows.length - 1];
    const rA = rsiArr[a.idx];
    const rB = rsiArr[b.idx];
    if (rA == null || rB == null) return { ok: false };

    const gapOk = (b.idx - a.idx) >= minSwingGap;
    const ok = gapOk && b.price < a.price && rB > rA;
    return { ok, a, b, rA, rB, gap: b.idx - a.idx, minSwingGap };
  }

  if (type === 'BEAR') {
    if (highs.length < 2) return { ok: false };
    const a = highs[highs.length - 2];
    const b = highs[highs.length - 1];
    const rA = rsiArr[a.idx];
    const rB = rsiArr[b.idx];
    if (rA == null || rB == null) return { ok: false };

    const gapOk = (b.idx - a.idx) >= minSwingGap;
    const ok = gapOk && b.price > a.price && rB < rA;
    return { ok, a, b, rA, rB, gap: b.idx - a.idx, minSwingGap };
  }

  return { ok: false };
}

export function isEntrySignalV2({ bias, candles30, candles15, candles5 }) {
  const profile = (process.env.ANALYZE_PROFILE ?? 'strict').toLowerCase();

  if (bias !== 'BUY' && bias !== 'SELL') return { ok: false, reason: 'BIAS_WAIT' };

  const easyMode = process.env.ANALYZE_EASY_MODE === '1';
  if (easyMode) {
    const c5 = last(candles5);
    if (c5?.rsi == null) return { ok: false, reason: 'EASY_NO_RSI_5M' };
    if (bias === 'BUY' && c5.rsi < 50) return { ok: false, reason: 'EASY_NO_MOMENTUM' };
    if (bias === 'SELL' && c5.rsi > 50) return { ok: false, reason: 'EASY_NO_MOMENTUM' };
    return { ok: true, mode: 'EASY', note: 'ANALYZE_EASY_MODE=1 (relaxed rules)' };
  }

  if (profile === 'scalp') {
    const c15 = last(candles15);
    const c5 = last(candles5);

    if (!c15 || !c5) return { ok: false, reason: 'MISSING_CANDLES' };

    const closes15 = candles15.map(c => c.close);
    const rsi15 = calcRsi(closes15, 14);

    const ma50_15 = calcSma(closes15, 50);
    const slope50 = maSlope(ma50_15, 10);
    const window = candles15.slice(Math.max(0, candles15.length - 40));
    const rangePct = window.length
      ? (Math.max(...window.map(c => c.high)) - Math.min(...window.map(c => c.low))) / c15.close
      : null;

    if (slope50 != null && Math.abs(slope50) < Number(process.env.SCALP_MIN_ABS_SLOPE50 ?? 0.0015)) {
      return { ok: false, reason: 'SCALP_FLAT_SLOPE', details: { slope50 } };
    }
    if (rangePct != null && rangePct < Number(process.env.SCALP_MIN_RANGE_PCT ?? 0.0045)) {
      return { ok: false, reason: 'SCALP_TIGHT_RANGE', details: { rangePct } };
    }

    const struct15 = structureLabel(candles15);
    const struct30 = structureLabel(candles30);

    if (bias === 'BUY' && (struct15 === 'BEAR' || struct30 === 'BEAR')) {
      return { ok: false, reason: 'SCALP_AGAINST_STRUCTURE', details: { struct15, struct30 } };
    }
    if (bias === 'SELL' && (struct15 === 'BULL' || struct30 === 'BULL')) {
      return { ok: false, reason: 'SCALP_AGAINST_STRUCTURE', details: { struct15, struct30 } };
    }

    const div = rsiDivergence({ candles: candles15, rsiArr: rsi15, type: bias === 'BUY' ? 'BULL' : 'BEAR' });
    if (!div.ok) return { ok: false, reason: 'SCALP_NO_RSI_DIVERGENCE' };
    if (!inMaZone(c15, Number(process.env.SCALP_MA_ZONE_PCT ?? 0.006))) {
      return { ok: false, reason: 'SCALP_NOT_AT_MA_ZONE' };
    }

    if (c5?.rsi == null) return { ok: false, reason: 'NO_RSI_5M' };
    const buyMin = Number(process.env.SCALP_RSI5_BUY_MIN ?? 52);
    const sellMax = Number(process.env.SCALP_RSI5_SELL_MAX ?? 48);
    if (bias === 'BUY' && c5.rsi < buyMin) return { ok: false, reason: 'SCALP_NO_LTF_MOMENTUM', details: { rsi5: c5.rsi } };
    if (bias === 'SELL' && c5.rsi > sellMax) return { ok: false, reason: 'SCALP_NO_LTF_MOMENTUM', details: { rsi5: c5.rsi } };

    const volEnabled = (process.env.VOLUME_FILTER_ENABLED ?? '1') === '1';
    if (volEnabled) {
      const tf = String(process.env.VOLUME_FILTER_TF ?? '5m').toLowerCase();
      const lookback = Number(process.env.VOLUME_LOOKBACK ?? 20);
      const mult = Number(process.env.SCALP_VOLUME_MULT ?? process.env.VOLUME_MULT ?? 1.5);

      const v5 = volumeSpikeOk(candles5, { lookback, mult });
      const v15 = volumeSpikeOk(candles15, { lookback, mult });

      if (tf === '5m' && !v5.ok) return { ok: false, reason: 'SCALP_VOLUME_FILTER_5M', details: v5 };
      if (tf === '15m' && !v15.ok) return { ok: false, reason: 'SCALP_VOLUME_FILTER_15M', details: v15 };
      if (tf === 'both' && (!v5.ok || !v15.ok)) {
        return { ok: false, reason: 'SCALP_VOLUME_FILTER_BOTH', details: { v5, v15 } };
      }
    }

    return {
      ok: true,
      mode: 'SCALP',
      struct15,
      struct30,
      div: {
        type: bias === 'BUY' ? 'BULL' : 'BEAR',
        priceA: div.a.price,
        priceB: div.b.price,
        rsiA: div.rA,
        rsiB: div.rB,
      },
      sidewaysDetails: { slope50, rangePct },
    };
  }

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

  const struct30 = structureLabel(candles30);
  const struct15 = structureLabel(candles15);

  if (bias === 'BUY' && !(struct30 === 'BULL' || struct15 === 'BULL')) {
    return { ok: false, reason: 'STRUCTURE_NOT_BULL', details: { struct30, struct15 } };
  }
  if (bias === 'SELL' && !(struct30 === 'BEAR' || struct15 === 'BEAR')) {
    return { ok: false, reason: 'STRUCTURE_NOT_BEAR', details: { struct30, struct15 } };
  }

  const div = rsiDivergence({ candles: candles15, rsiArr: rsi15, type: bias === 'BUY' ? 'BULL' : 'BEAR' });
  if (!div.ok) {
    return { ok: false, reason: 'NO_RSI_DIVERGENCE' };
  }

  if (!inMaZone(c15, 0.006)) {
    return { ok: false, reason: 'NOT_AT_MA_ZONE' };
  }

  if (c5.rsi == null) return { ok: false, reason: 'NO_RSI_5M' };
  if (bias === 'BUY' && c5.rsi < 52) return { ok: false, reason: 'NO_LTF_MOMENTUM' };
  if (bias === 'SELL' && c5.rsi > 48) return { ok: false, reason: 'NO_LTF_MOMENTUM' };

  const volEnabled = (process.env.VOLUME_FILTER_ENABLED ?? '1') === '1';
  if (volEnabled) {
    const tf = String(process.env.VOLUME_FILTER_TF ?? '5m').toLowerCase();
    const lookback = Number(process.env.VOLUME_LOOKBACK ?? 20);
    const mult = Number(process.env.VOLUME_MULT ?? 1.5);

    const v5 = volumeSpikeOk(candles5, { lookback, mult });
    const v15 = volumeSpikeOk(candles15, { lookback, mult });

    if (tf === '5m' && !v5.ok) return { ok: false, reason: 'VOLUME_FILTER_5M', details: v5 };
    if (tf === '15m' && !v15.ok) return { ok: false, reason: 'VOLUME_FILTER_15M', details: v15 };
    if (tf === 'both' && (!v5.ok || !v15.ok)) {
      return { ok: false, reason: 'VOLUME_FILTER_BOTH', details: { v5, v15 } };
    }
  }

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

  const c15 = last(data['15m']);
  const c30 = last(data['30m']);
  const c1h = last(data['1h']);
  const c4h = last(data['4h']);
  const c1d = last(data['1d']);

  const trend1d = trendLabel(c1d ?? {});
  const trend4h = trendLabel(c4h ?? {});
  const trend1h = trendLabel(c1h ?? {});

  const profile = (process.env.ANALYZE_PROFILE ?? 'strict').toLowerCase();

  let bias = decideBias(trend1d, trend4h, trend1h);
  if (profile === 'scalp') {
    if (trend4h === 'BULL' && trend1h === 'BULL') bias = 'BUY';
    else if (trend4h === 'BEAR' && trend1h === 'BEAR') bias = 'SELL';
    else bias = 'WAIT';
  }

  if (process.env.ANALYZE_EASY_MODE === '1' && bias === 'WAIT') {
    if (trend1d === 'BULL') bias = 'BUY';
    else if (trend1d === 'BEAR') bias = 'SELL';
  }

  let entryCheck = isEntrySignalV2({ bias, candles30: data['30m'], candles15: data['15m'], candles5: data['5m'] });
  const easyMode = process.env.ANALYZE_EASY_MODE === '1';

  let setup = null;

  if (!easyMode && profile === 'scalp' && bias !== 'WAIT' && entryCheck.ok) {
    const entry = Number(c5?.close);
    if (Number.isFinite(entry) && entry > 0) {
      const n = Number(process.env.SCALP_STRUCTURE_SL_BARS ?? 5);
      const bars = Math.max(3, Math.min(n, 8));
      const slice = data['5m'].slice(Math.max(0, data['5m'].length - bars));

      const highs5 = data['5m'].map(c => c.high);
      const lows5 = data['5m'].map(c => c.low);
      const closes5 = data['5m'].map(c => c.close);
      const atr5 = calcAtr(highs5, lows5, closes5, 14);
      const atrNow5 = atr5[atr5.length - 1];
      const slAtrMult = Number(process.env.SCALP_SL_ATR_MULT ?? 0.35);
      const slBuffer = atrNow5 != null ? atrNow5 * Math.max(0, slAtrMult) : 0;

      const rMult = Number(process.env.SCALP_TP_R_MULT ?? 1.8);
      const rrMult = Math.max(1.0, Math.min(rMult, 3.0));

      if (bias === 'BUY') {
        const structLow = Math.min(...slice.map(c => c.low));
        const sl = structLow - slBuffer;
        const risk = entry - sl;
        if (risk > 0) {
          const tp = entry + rrMult * risk;
          setup = { action: 'BUY', entry, sl, tp, rr: (tp - entry) / risk, reasons: { entryCheck, sltpMeta: { bars, structLow, atrNow5, slBuffer, rrMult } } };
        }
      } else {
        const structHigh = Math.max(...slice.map(c => c.high));
        const sl = structHigh + slBuffer;
        const risk = sl - entry;
        if (risk > 0) {
          const tp = entry - rrMult * risk;
          setup = { action: 'SELL', entry, sl, tp, rr: (entry - tp) / risk, reasons: { entryCheck, sltpMeta: { bars, structHigh, atrNow5, slBuffer, rrMult } } };
        }
      }
    }
  }

  if (easyMode && bias !== 'WAIT') {
    const base = Number(c5?.close);
    if (Number.isFinite(base) && base > 0) {
      const entry = base;
      const riskPct = Number(process.env.EASY_RISK_PCT ?? 0.003);
      const riskAbs = entry * Math.max(0.0001, Math.min(riskPct, 0.02));

      if (bias === 'BUY') {
        const sl = entry - riskAbs;
        const tp = entry + 2 * riskAbs;
        setup = { action: 'BUY', entry, sl, tp, rr: 2, reasons: { entryCheck: { ...entryCheck, ok: true, mode: 'EASY_FORCE' } } };
      } else {
        const sl = entry + riskAbs;
        const tp = entry - 2 * riskAbs;
        setup = { action: 'SELL', entry, sl, tp, rr: 2, reasons: { entryCheck: { ...entryCheck, ok: true, mode: 'EASY_FORCE' } } };
      }
      entryCheck = { ...entryCheck, ok: true, mode: 'EASY_FORCE' };
    }
  }

  if (!setup && bias !== 'WAIT' && entryCheck.ok) {
    const base = c5.close;
    const offsetPctRaw = Number(process.env.ENTRY_OFFSET_PCT ?? 0.0005);
    const offsetMaxRaw = Number(process.env.ENTRY_OFFSET_MAX_PCT ?? 0.002);
    const offsetPct = Math.min(Math.max(offsetPctRaw, 0), Math.max(offsetMaxRaw, 0));
    const entry = bias === 'BUY' ? base * (1 - offsetPct) : base * (1 + offsetPct);

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
      let sl = swing - slBuffer;
      if (minRiskAbs > 0) sl = Math.min(sl, entry - minRiskAbs);
      const risk = entry - sl;
      const baseTp = entry + 2 * risk;
      const tp = Math.max(baseTp, entry + Math.max(minTpAbs, tpMinAtr));
      if (risk > 0) setup = { action: 'BUY', entry, sl, tp, rr: (tp - entry) / risk, reasons: { entryCheck, sltpMeta: { swing, slBuffer, atrNow15, lookback, minRiskPct, minTpPct, tpMinAtr } } };
    } else {
      const swing = recentSwingHigh(data['15m'], lookback);
      let sl = swing + slBuffer;
      if (minRiskAbs > 0) sl = Math.max(sl, entry + minRiskAbs);
      const risk = sl - entry;
      const baseTp = entry - 2 * risk;
      const tp = Math.min(baseTp, entry - Math.max(minTpAbs, tpMinAtr));
      if (risk > 0) setup = { action: 'SELL', entry, sl, tp, rr: (entry - tp) / risk, reasons: { entryCheck, sltpMeta: { swing, slBuffer, atrNow15, lookback, minRiskPct, minTpPct, tpMinAtr } } };
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
