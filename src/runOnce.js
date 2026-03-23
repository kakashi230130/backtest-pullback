import { config } from './config.js';
import { pingDb } from './db.js';
import { logger } from './logger.js';
import { syncSymbolInterval } from './syncCandles.js';

const interval = process.argv[2];
if (!interval) {
  console.error('Usage: node src/runOnce.js <interval>  (e.g. 5m, 15m, 1h)');
  process.exit(1);
}

async function main() {
  await pingDb();
  for (const symbol of config.symbols) {
    await syncSymbolInterval({ symbol, interval });
  }
  logger.info('Done');
  process.exit(0);
}

main().catch((err) => {
  logger.error({ err }, 'RunOnce failed');
  process.exit(1);
});
