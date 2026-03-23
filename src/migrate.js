import { pool } from './db.js';
import { logger } from './logger.js';

// Simple migration (idempotent)
const sql = `
CREATE TABLE IF NOT EXISTS candles (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  symbol VARCHAR(20) NOT NULL,
  interval_code VARCHAR(5) NOT NULL,
  open_time BIGINT NOT NULL,
  open DECIMAL(30, 12) NOT NULL,
  high DECIMAL(30, 12) NOT NULL,
  low DECIMAL(30, 12) NOT NULL,
  close DECIMAL(30, 12) NOT NULL,
  volume DECIMAL(30, 12) NOT NULL,
  close_time BIGINT NOT NULL,
  quote_asset_volume DECIMAL(30, 12) NOT NULL,
  number_of_trades INT NOT NULL,
  taker_buy_base_asset_volume DECIMAL(30, 12) NOT NULL,
  taker_buy_quote_asset_volume DECIMAL(30, 12) NOT NULL,
  is_closed TINYINT(1) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_symbol_interval_open (symbol, interval_code, open_time),
  KEY idx_symbol_interval_close (symbol, interval_code, close_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`;

async function main() {
  logger.info('Running migration...');
  await pool.query(sql);
  logger.info('Migration done.');
  await pool.end();
}

main().catch(async (err) => {
  logger.error({ err }, 'Migration failed');
  try { await pool.end(); } catch {}
  process.exit(1);
});
