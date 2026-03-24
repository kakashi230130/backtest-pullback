import cron from 'node-cron';
import { logger } from './logger.js';
import { pool } from './db.js';

// Run analyze every 30 minutes.
// NOTE: This schedules in the server's local timezone.

let running = false;

async function runOnce(trigger = 'manual') {
  if (running) {
    logger.warn({ trigger }, 'Analyze is still running; skip this tick');
    return;
  }

  const tickId = `analyze:${Date.now()}`;
  const startedAt = Date.now();
  running = true;

  logger.info({ tickId, trigger }, 'Analyze tick start');

  try {
    // Dynamic import to avoid module caching issues if you edit code while daemon runs.
    await import(`./analyzeAndAct.js?ts=${Date.now()}`);
    // analyzeAndAct executes immediately on import (it calls main()).
    logger.info({ tickId, ms: Date.now() - startedAt }, 'Analyze tick done');
  } catch (err) {
    logger.error({ err, tickId, ms: Date.now() - startedAt }, 'Analyze tick failed');
  } finally {
    running = false;
  }
}

async function shutdown(signal) {
  logger.info({ signal }, 'Shutting down scheduler');
  try {
    await pool.end();
  } catch (err) {
    logger.warn({ err }, 'Failed to close DB pool on shutdown');
  }
  process.exit(0);
}

async function main() {
  logger.info('Analyze scheduler started: every 5 minutes + delay seconds');

  // graceful shutdown (Ctrl+C / service stop)
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  // run immediately on start
  await runOnce('startup');

  // 5m + delay seconds (default 20s)
  const delaySec = Number(process.env.ANALYZE_DELAY_SECONDS ?? 20);
  const sec = Number.isFinite(delaySec) ? Math.max(0, Math.min(delaySec, 55)) : 20;

  cron.schedule(`${sec} */5 * * * *`, () => {
    logger.info({ sec }, 'Analyze cron tick');
    runOnce('cron');
  });
}

main().catch((err) => {
  logger.error({ err }, 'Scheduler fatal');
  process.exit(1);
});
