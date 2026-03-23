import { config } from './config.js';
import { pingDb } from './db.js';
import { logger } from './logger.js';
import { fetchKlines, onlyClosedKlines } from './binanceClient.js';
import { mapKline, upsertCandles } from './candlesRepo.js';
import { addIndicatorsToCandleRows } from './indicators.js';

// Fetches latest candles from Binance, computes RSI/MA, and stores ~200 newest rows per interval.

const intervals = ['5m', '15m', '30m', '1h', '4h', '1d'];
const KEEP = 200;
// Need enough data for MA200 + RSI14 warmup.
const FETCH = 350;

async function backfillSymbolInterval(symbol, interval) {
  logger.info({ symbol, interval }, 'Fetching klines...');
  const klinesRaw = await fetchKlines({ symbol, interval, limit: FETCH });
  const klines = onlyClosedKlines(klinesRaw);
  if (!klines.length) return;

  let rows = klines.map(k => mapKline(symbol, interval, k));
  rows.sort((a, b) => a.open_time - b.open_time);

  addIndicatorsToCandleRows(rows);

  // keep newest ~200
  rows = rows.slice(Math.max(0, rows.length - KEEP));

  // Convert indicator numbers to strings (DECIMAL-friendly) or null
  for (const r of rows) {
    for (const key of ['rsi', 'ma20', 'ma50', 'ma200']) {
      if (r[key] === null) continue;
      // Keep some precision, MySQL DECIMAL will store exact.
      r[key] = Number(r[key]).toFixed(8);
    }
  }

  await upsertCandles(rows);
  logger.info({ symbol, interval, stored: rows.length }, 'Backfill stored');
}

async function main() {
  await pingDb();
  for (const symbol of config.symbols) {
    for (const interval of intervals) {
      await backfillSymbolInterval(symbol, interval);
    }
  }
  logger.info('Backfill latest200 done');
  process.exit(0);
}

main().catch((err) => {
  logger.error({ err }, 'Backfill failed');
  process.exit(1);
});
