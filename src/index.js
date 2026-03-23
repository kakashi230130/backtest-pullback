import { startCron } from './cron.js';
import { logger } from './logger.js';

startCron().catch((err) => {
  logger.error({ err }, 'Fatal startup error');
  process.exit(1);
});
