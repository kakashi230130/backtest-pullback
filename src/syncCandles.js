import { fetchKlines, onlyClosedKlines } from './binanceClient.js';
import { getLastOpenTime, getRecentClosesBefore, mapKline, upsertCandles } from './candlesRepo.js';
import { intervalToMs } from './intervals.js';
import { logger } from './logger.js';
import { addIndicatorsToCandleRows } from './indicators.js';

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

export async function syncSymbolInterval({ symbol, interval, lookbackCandles = 5, maxPages = 10 }) {
  const stepMs = intervalToMs(interval);

  const lastOpen = await getLastOpenTime(symbol, interval);

  // Start from a bit earlier to safely re-fetch the last few candles.
  let startTime;
  if (lastOpen === null) {
    // First run: fetch last ~1000 candles by leaving startTime undefined.
    startTime = undefined;
  } else {
    startTime = Math.max(0, lastOpen - lookbackCandles * stepMs);
  }

  let pages = 0;
  let total = 0;

  while (pages < maxPages) {
    pages += 1;

    const klinesRaw = await fetchKlines({ symbol, interval, startTime, limit: 1000 });
    const klines = onlyClosedKlines(klinesRaw);
    if (!klines.length) {
      // If the API only returned the currently-forming candle, we are caught up.
      break;
    }

    let rows = klines.map(k => mapKline(symbol, interval, k));
    rows.sort((a, b) => a.open_time - b.open_time);

    // Compute indicators with proper warmup.
    // For regular cron runs, Binance will return only a small number of new candles,
    // so we load recent closes from DB to compute RSI/MA correctly.
    if (startTime !== undefined) {
      const warmup = await getRecentClosesBefore(symbol, interval, rows[0].open_time, 260);

      const combined = [
        ...warmup.map(w => ({ close: w.close })),
        ...rows.map(r => ({ close: Number(r.close) })),
      ];

      addIndicatorsToCandleRows(combined);

      // Copy computed indicator values for the fetched rows (tail section)
      const offset = combined.length - rows.length;
      for (let i = 0; i < rows.length; i++) {
        const c = combined[offset + i];
        rows[i].rsi = c.rsi;
        rows[i].ma20 = c.ma20;
        rows[i].ma50 = c.ma50;
        rows[i].ma200 = c.ma200;
      }
    } else {
      // First run / large fetch window
      const window = rows.map(r => ({ close: Number(r.close) }));
      addIndicatorsToCandleRows(window);
      for (let i = 0; i < rows.length; i++) {
        rows[i].rsi = window[i].rsi;
        rows[i].ma20 = window[i].ma20;
        rows[i].ma50 = window[i].ma50;
        rows[i].ma200 = window[i].ma200;
      }
    }

    // Convert indicator numbers to strings (DECIMAL-friendly) or null
    for (const r of rows) {
      for (const key of ['rsi', 'ma20', 'ma50', 'ma200']) {
        if (r[key] === null) continue;
        r[key] = Number(r[key]).toFixed(8);
      }
    }

    await upsertCandles(rows);
    total += rows.length;

    const last = rows[rows.length - 1];
    const nextStart = last.open_time + stepMs;

    // If we didn't specify startTime (first run), we just did the latest page.
    // Stop here to avoid backfilling endlessly.
    if (startTime === undefined) break;

    // If the next start doesn't move forward, stop.
    if (nextStart <= startTime) break;

    startTime = nextStart;

    // gentle pacing (Binance rate limits)
    await sleep(250);

    // If we received less than limit, likely caught up.
    if (rows.length < 1000) break;
  }

  logger.info({ symbol, interval, pages, total }, 'Sync done');
  return { symbol, interval, pages, total };
}
