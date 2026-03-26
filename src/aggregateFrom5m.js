import 'dotenv/config';

import { pool } from './db.js';
import { upsertCandles, getRecentClosesBefore } from './candlesRepo.js';
import { intervalToMs } from './intervals.js';
import { addIndicatorsToCandleRows } from './indicators.js';

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    const key = a.slice(2);
    const val = argv[i + 1];
    if (val == null || val.startsWith('--')) {
      out[key] = true;
    } else {
      out[key] = val;
      i++;
    }
  }
  return out;
}

function parseTimeMs(x) {
  if (x == null) return null;
  if (/^\d+$/.test(String(x))) return Number(x);
  const t = Date.parse(String(x));
  if (!Number.isFinite(t)) throw new Error(`Invalid time: ${x}`);
  return t;
}

function toNum(x) {
  if (x == null) return 0;
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

function toDecStr(x, dp = 12) {
  const n = Number(x);
  if (!Number.isFinite(n)) return null;
  return n.toFixed(dp);
}

function bucketStart(openTime, tfMs) {
  return Math.floor(Number(openTime) / tfMs) * tfMs;
}

async function load5m({ symbol, startTime, endTime }) {
  const [rows] = await pool.query(
    `
    SELECT open_time, open, high, low, close, volume,
           close_time, quote_asset_volume, number_of_trades,
           taker_buy_base_asset_volume, taker_buy_quote_asset_volume
    FROM candles
    WHERE symbol=:symbol
      AND interval_code='5m'
      AND open_time>=:startTime
      AND open_time<=:endTime
    ORDER BY open_time ASC
    `,
    { symbol, startTime, endTime },
  );

  return rows.map(r => ({
    open_time: Number(r.open_time),
    open: Number(r.open),
    high: Number(r.high),
    low: Number(r.low),
    close: Number(r.close),
    volume: Number(r.volume),
    close_time: Number(r.close_time),
    quote_asset_volume: Number(r.quote_asset_volume),
    number_of_trades: Number(r.number_of_trades),
    taker_buy_base_asset_volume: Number(r.taker_buy_base_asset_volume),
    taker_buy_quote_asset_volume: Number(r.taker_buy_quote_asset_volume),
  }));
}

function aggregateBuckets({ symbol, baseRows, targetInterval }) {
  const baseMs = intervalToMs('5m');
  const tfMs = intervalToMs(targetInterval);
  const need = Math.round(tfMs / baseMs);
  if (need < 1 || need !== tfMs / baseMs) throw new Error(`Target interval must be multiple of 5m: ${targetInterval}`);

  const out = [];

  let curStart = null;
  let buf = [];

  function flush() {
    if (!buf.length) return;
    if (buf.length !== need) {
      buf = [];
      return;
    }

    for (let i = 1; i < buf.length; i++) {
      if (buf[i].open_time !== buf[i - 1].open_time + baseMs) {
        buf = [];
        return;
      }
    }

    const first = buf[0];
    const last = buf[buf.length - 1];

    const open = first.open;
    const close = last.close;
    const high = Math.max(...buf.map(x => x.high));
    const low = Math.min(...buf.map(x => x.low));

    const volume = buf.reduce((a, x) => a + toNum(x.volume), 0);
    const qav = buf.reduce((a, x) => a + toNum(x.quote_asset_volume), 0);
    const trades = buf.reduce((a, x) => a + toNum(x.number_of_trades), 0);
    const tbb = buf.reduce((a, x) => a + toNum(x.taker_buy_base_asset_volume), 0);
    const tbq = buf.reduce((a, x) => a + toNum(x.taker_buy_quote_asset_volume), 0);

    out.push({
      symbol,
      interval_code: targetInterval,
      open_time: curStart,
      open: toDecStr(open, 12),
      high: toDecStr(high, 12),
      low: toDecStr(low, 12),
      close: toDecStr(close, 12),
      volume: toDecStr(volume, 12),
      close_time: curStart + tfMs - 1,
      quote_asset_volume: toDecStr(qav, 12),
      number_of_trades: Math.round(trades),
      taker_buy_base_asset_volume: toDecStr(tbb, 12),
      taker_buy_quote_asset_volume: toDecStr(tbq, 12),
      is_closed: 1,
      rsi: null,
      ma20: null,
      ma50: null,
      ma200: null,
    });

    buf = [];
  }

  for (const r of baseRows) {
    const bs = bucketStart(r.open_time, tfMs);

    if (curStart == null) {
      curStart = bs;
      buf = [r];
      continue;
    }

    if (bs !== curStart) {
      flush();
      curStart = bs;
      buf = [r];
    } else {
      buf.push(r);
    }
  }

  flush();
  return out;
}

async function addIndicators({ symbol, interval, rows }) {
  if (!rows.length) return rows;

  const warmup = await getRecentClosesBefore(symbol, interval, rows[0].open_time, 260);
  const combined = [
    ...warmup.map(w => ({ close: Number(w.close) })),
    ...rows.map(r => ({ close: Number(r.close) })),
  ];

  addIndicatorsToCandleRows(combined);

  const offset = combined.length - rows.length;
  for (let i = 0; i < rows.length; i++) {
    const c = combined[offset + i];
    rows[i].rsi = c.rsi == null ? null : Number(c.rsi).toFixed(8);
    rows[i].ma20 = c.ma20 == null ? null : Number(c.ma20).toFixed(8);
    rows[i].ma50 = c.ma50 == null ? null : Number(c.ma50).toFixed(8);
    rows[i].ma200 = c.ma200 == null ? null : Number(c.ma200).toFixed(8);
  }

  return rows;
}

async function main() {
  const args = parseArgs(process.argv);
  const symbol = args.symbol ?? process.env.BACKTEST_SYMBOL;
  if (!symbol) throw new Error('Missing --symbol');

  const startTime = parseTimeMs(args.start ?? args.startTime ?? (Date.now() - 30 * 24 * 60 * 60 * 1000));
  const endTime = parseTimeMs(args.end ?? args.endTime ?? Date.now());

  const intervals = String(args.intervals ?? '15m,30m,1h,4h,1d')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);

  const warmupMs = intervalToMs('1d');
  const baseRows = await load5m({ symbol, startTime: startTime - warmupMs, endTime });
  if (!baseRows.length) {
    console.warn('No 5m candles found');
    return;
  }

  const summary = [];

  for (const itv of intervals) {
    const agg = aggregateBuckets({ symbol, baseRows, targetInterval: itv });
    const filtered = agg.filter(r => r.open_time >= startTime && r.open_time <= endTime);

    await addIndicators({ symbol, interval: itv, rows: filtered });

    const { insertedOrUpdated } = await upsertCandles(filtered);
    summary.push({ interval: itv, candles: filtered.length, insertedOrUpdated });
  }

  console.log(JSON.stringify({ ok: true, symbol, startTime, endTime, summary }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
