import 'dotenv/config';

import { config } from './config.js';
import { logger } from './logger.js';
import { fetchKlines, onlyClosedKlines } from './binanceClient.js';
import { getRecentClosesBefore, mapKline, upsertCandles } from './candlesRepo.js';
import { intervalToMs } from './intervals.js';
import { addIndicatorsToCandleRows } from './indicators.js';

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

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

function parseIntervals(x) {
  if (!x) return null;
  return String(x)
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
}

function toFixedOrNull(x, dp = 8) {
  if (x == null) return null;
  const n = Number(x);
  if (!Number.isFinite(n)) return null;
  return n.toFixed(dp);
}

async function backfillSymbolIntervalRange({ symbol, interval, startTime, endTime, maxPages = 1000, sleepMs = 250 }) {
  const stepMs = intervalToMs(interval);
  let start = Number(startTime);
  const end = Number(endTime);
  if (!Number.isFinite(start) || !Number.isFinite(end)) throw new Error('startTime/endTime invalid');

  // Align start to the interval boundary to avoid partial buckets.
  start = Math.floor(start / stepMs) * stepMs;

  let pages = 0;
  let total = 0;

  while (pages < maxPages && start <= end) {
    pages += 1;

    // Binance limit = 1000; request within [start, end]
    const klinesRaw = await fetchKlines({ symbol, interval, startTime: start, endTime: end, limit: 1000 });
    const klines = onlyClosedKlines(klinesRaw);
    if (!klines.length) break;

    let rows = klines.map(k => mapKline(symbol, interval, k));
    rows.sort((a, b) => a.open_time - b.open_time);

    // Warmup closes from DB (best effort) to compute indicators for this page.
    const warmup = await getRecentClosesBefore(symbol, interval, rows[0].open_time, 260);

    const combined = [
      ...warmup.map(w => ({ close: w.close })),
      ...rows.map(r => ({ close: Number(r.close) })),
    ];

    addIndicatorsToCandleRows(combined);

    const offset = combined.length - rows.length;
    for (let i = 0; i < rows.length; i++) {
      const c = combined[offset + i];
      rows[i].rsi = toFixedOrNull(c.rsi, 8);
      rows[i].ma20 = toFixedOrNull(c.ma20, 8);
      rows[i].ma50 = toFixedOrNull(c.ma50, 8);
      rows[i].ma200 = toFixedOrNull(c.ma200, 8);
    }

    await upsertCandles(rows);
    total += rows.length;

    const last = rows[rows.length - 1];
    const nextStart = Number(last.open_time) + stepMs;

    logger.info({ symbol, interval, pages, got: rows.length, total, nextStart }, 'Backfill range page done');

    if (!(nextStart > start)) break;
    start = nextStart;

    if (rows.length < 1000) break; // likely caught up within endTime

    if (sleepMs > 0) await sleep(sleepMs);
  }

  return { symbol, interval, pages, total };
}

async function main() {
  const args = parseArgs(process.argv);

  const symbolArg = args.symbol ?? null;
  const symbols = symbolArg
    ? [symbolArg]
    : (Array.isArray(config.symbols) && config.symbols.length ? config.symbols : []);

  const intervals = parseIntervals(args.interval ?? args.intervals) ?? ['5m'];

  const startTime = parseTimeMs(args.start ?? args.startTime);
  const endTime = parseTimeMs(args.end ?? args.endTime);

  if (!symbols.length) throw new Error('Missing --symbol (or config.symbols is empty)');
  if (!startTime) throw new Error('Missing --start (ms or ISO)');
  if (!endTime) throw new Error('Missing --end (ms or ISO)');

  const maxPages = Number(args.maxPages ?? 2000);
  const sleepMs = Number(args.sleepMs ?? 250);

  const results = [];
  for (const symbol of symbols) {
    for (const interval of intervals) {
      results.push(await backfillSymbolIntervalRange({ symbol, interval, startTime, endTime, maxPages, sleepMs }));
    }
  }

  console.log(JSON.stringify({ ok: true, startTime, endTime, results }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
