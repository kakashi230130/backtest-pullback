// Technical indicators (simple implementations)

export function sma(values, period) {
  const out = Array(values.length).fill(null);
  if (period <= 0) throw new Error('period must be > 0');
  let sum = 0;
  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    sum += v;
    if (i >= period) sum -= values[i - period];
    if (i >= period - 1) out[i] = sum / period;
  }
  return out;
}

// RSI using Wilder's smoothing
export function rsi(values, period = 14) {
  const out = Array(values.length).fill(null);
  if (values.length < period + 1) return out;

  let gainSum = 0;
  let lossSum = 0;

  for (let i = 1; i <= period; i++) {
    const diff = values[i] - values[i - 1];
    if (diff >= 0) gainSum += diff;
    else lossSum += -diff;
  }

  let avgGain = gainSum / period;
  let avgLoss = lossSum / period;

  // First RSI value at index = period
  out[period] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);

  for (let i = period + 1; i < values.length; i++) {
    const diff = values[i] - values[i - 1];
    const gain = diff > 0 ? diff : 0;
    const loss = diff < 0 ? -diff : 0;

    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;

    out[i] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
  }

  return out;
}

export function atr(highs, lows, closes, period = 14) {
  const out = Array(highs.length).fill(null);
  if (highs.length < period + 1) return out;

  const tr = [];
  for (let i = 0; i < highs.length; i++) {
    if (i === 0) {
      tr.push(highs[i] - lows[i]);
    } else {
      const hl = highs[i] - lows[i];
      const hc = Math.abs(highs[i] - closes[i - 1]);
      const lc = Math.abs(lows[i] - closes[i - 1]);
      tr.push(Math.max(hl, hc, lc));
    }
  }

  // Wilder smoothing
  let sum = 0;
  for (let i = 0; i < period; i++) sum += tr[i];
  let prev = sum / period;
  out[period - 1] = prev;

  for (let i = period; i < tr.length; i++) {
    prev = (prev * (period - 1) + tr[i]) / period;
    out[i] = prev;
  }

  return out;
}

// ADX (Wilder), returns array of ADX values (null until enough warmup)
export function adx(highs, lows, closes, period = 14) {
  const n = highs.length;
  const out = Array(n).fill(null);
  if (n < period * 2) return out;

  const plusDM = Array(n).fill(0);
  const minusDM = Array(n).fill(0);
  const tr = Array(n).fill(0);

  for (let i = 1; i < n; i++) {
    const upMove = highs[i] - highs[i - 1];
    const downMove = lows[i - 1] - lows[i];

    plusDM[i] = upMove > downMove && upMove > 0 ? upMove : 0;
    minusDM[i] = downMove > upMove && downMove > 0 ? downMove : 0;

    const hl = highs[i] - lows[i];
    const hc = Math.abs(highs[i] - closes[i - 1]);
    const lc = Math.abs(lows[i] - closes[i - 1]);
    tr[i] = Math.max(hl, hc, lc);
  }

  // Wilder smoothing for TR and DMs
  let tr14 = 0;
  let p14 = 0;
  let m14 = 0;
  for (let i = 1; i <= period; i++) {
    tr14 += tr[i];
    p14 += plusDM[i];
    m14 += minusDM[i];
  }

  const dx = Array(n).fill(null);

  for (let i = period + 1; i < n; i++) {
    tr14 = tr14 - tr14 / period + tr[i];
    p14 = p14 - p14 / period + plusDM[i];
    m14 = m14 - m14 / period + minusDM[i];

    const plusDI = tr14 === 0 ? 0 : (100 * p14) / tr14;
    const minusDI = tr14 === 0 ? 0 : (100 * m14) / tr14;
    const denom = plusDI + minusDI;
    dx[i] = denom === 0 ? 0 : (100 * Math.abs(plusDI - minusDI)) / denom;
  }

  // First ADX = SMA of DX over period (starting at index period+1)
  let start = period + 1;
  let sumDX = 0;
  let count = 0;
  for (let i = start; i < start + period; i++) {
    if (dx[i] == null) continue;
    sumDX += dx[i];
    count++;
  }
  if (!count) return out;
  let adxPrev = sumDX / count;
  out[start + period - 1] = adxPrev;

  for (let i = start + period; i < n; i++) {
    if (dx[i] == null) continue;
    adxPrev = (adxPrev * (period - 1) + dx[i]) / period;
    out[i] = adxPrev;
  }

  return out;
}

export function addIndicatorsToCandleRows(rows) {
  // rows must be sorted by open_time ascending
  const closes = rows.map(r => Number(r.close));

  const ma20 = sma(closes, 20);
  const ma50 = sma(closes, 50);
  const ma200 = sma(closes, 200);
  const rsi14 = rsi(closes, 14);

  for (let i = 0; i < rows.length; i++) {
    rows[i].ma20 = ma20[i];
    rows[i].ma50 = ma50[i];
    rows[i].ma200 = ma200[i];
    rows[i].rsi = rsi14[i];
  }

  return rows;
}
