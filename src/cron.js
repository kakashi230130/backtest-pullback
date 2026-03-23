import cron from 'node-cron';
import { config } from './config.js';
import { logger } from './logger.js';
import { pingDb } from './db.js';
import { syncSymbolInterval } from './syncCandles.js';

// Schedules chosen to run shortly after candle close time.
// NOTE: we use 6-field cron (second minute hour day month weekday) to support second-level offsets.
const schedules = {
  // 5m + 5s
  '5m': '5 */5 * * * *',
  // 15m + 10s
  '15m': '10 */15 * * * *',
  // 30m + 15s
  '30m': '15 */30 * * * *',
  // 1h + 20s (at minute 0)
  '1h': '20 0 * * * *',
  // 4h + 25s (at minute 0)
  '4h': '25 0 */4 * * *',
  // 1d + 30s (00:00:30 server local time)
  '1d': '30 0 0 * * *',
};

async function runInterval(interval) {
  const tickId = `${interval}:${Date.now()}`;
  const startedAt = Date.now();
  logger.info({ tickId, interval, symbols: config.symbols }, 'Sync tick start');

  let ok = 0;
  let fail = 0;

  for (const symbol of config.symbols) {
    const s0 = Date.now();
    try {
      await syncSymbolInterval({ symbol, interval });
      ok++;
      logger.info({ tickId, interval, symbol, ms: Date.now() - s0 }, 'Sync symbol ok');
    } catch (err) {
      fail++;
      logger.error({ err, tickId, symbol, interval, ms: Date.now() - s0 }, 'Sync symbol error');
    }
  }

  logger.info(
    { tickId, interval, ok, fail, ms: Date.now() - startedAt },
    'Sync tick done',
  );
}

export async function startCron() {
  await pingDb();
  logger.info({ symbols: config.symbols }, 'DB connected. Starting cron jobs...');

  for (const [interval, expr] of Object.entries(schedules)) {
    cron.schedule(expr, async () => {
      logger.info({ interval }, 'Cron tick');
      try {
        await runInterval(interval);
      } catch (err) {
        logger.error({ err, interval }, 'Run interval failed');
      }
    });
    logger.info({ interval, expr }, 'Cron scheduled');
  }
}
