import { pool } from './db.js';

function normalizeInterval(itv) {
  const x = String(itv);
  if (!x) throw new Error('interval required');
  return x;
}

function toNum(x) {
  if (x == null) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

async function queryCandles({ symbol, interval, startTime, endTime, includeIndicators }) {
  const colsBase = 'open_time, open, high, low, close, volume';
  const colsInd = 'rsi, ma20, ma50, ma200';
  const cols = includeIndicators ? `${colsBase}, ${colsInd}` : colsBase;

  const [rows] = await pool.query(
    `
    SELECT ${cols}
    FROM candles
    WHERE symbol=:symbol
      AND interval_code=:interval
      AND open_time>=:startTime
      AND open_time<=:endTime
    ORDER BY open_time ASC
    `,
    { symbol, interval, startTime, endTime },
  );

  return rows.map(r => ({
    open_time: Number(r.open_time),
    open: Number(r.open),
    high: Number(r.high),
    low: Number(r.low),
    close: Number(r.close),
    volume: Number(r.volume),
    rsi: includeIndicators ? toNum(r.rsi) : null,
    ma20: includeIndicators ? toNum(r.ma20) : null,
    ma50: includeIndicators ? toNum(r.ma50) : null,
    ma200: includeIndicators ? toNum(r.ma200) : null,
  }));
}

export async function loadCandlesMultiTf({ symbol, intervals, startTime, endTime, warmupMs = 0 }) {
  const start = Number(startTime) - Number(warmupMs || 0);
  const end = Number(endTime);

  let includeIndicators = true;

  const out = {};
  try {
    for (const itv0 of intervals) {
      const interval = normalizeInterval(itv0);
      out[interval] = await queryCandles({ symbol, interval, startTime: start, endTime: end, includeIndicators });
    }
    return { data: out, indicatorsFromDb: true };
  } catch (err) {
    const msg = String(err?.message ?? '');
    const code = err?.code;
    if (code === 'ER_BAD_FIELD_ERROR' || msg.includes('Unknown column')) {
      includeIndicators = false;
      for (const k of Object.keys(out)) delete out[k];
      for (const itv0 of intervals) {
        const interval = normalizeInterval(itv0);
        out[interval] = await queryCandles({ symbol, interval, startTime: start, endTime: end, includeIndicators });
      }
      return { data: out, indicatorsFromDb: false };
    }
    throw err;
  }
}
