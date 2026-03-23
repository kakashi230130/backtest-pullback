import { pool } from './db.js';
import { logger } from './logger.js';

// FUTURES (USDT-M) open trades table (manual insert for now)
const sql = `
CREATE TABLE IF NOT EXISTS open_trades (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

  exchange VARCHAR(32) NOT NULL DEFAULT 'binance_usdtm_demo',
  symbol VARCHAR(20) NOT NULL,

  side ENUM('LONG','SHORT') NOT NULL,
  leverage INT NULL,

  entry_price DECIMAL(30, 12) NOT NULL,
  quantity DECIMAL(30, 12) NOT NULL,

  stop_loss DECIMAL(30, 12) NULL,
  take_profit DECIMAL(30, 12) NULL,

  -- Optional linkage to Binance entities (if you decide to sync later)
  position_side ENUM('BOTH','LONG','SHORT') NULL,
  order_id BIGINT NULL,
  client_order_id VARCHAR(64) NULL,

  status ENUM('OPEN','CLOSED','CANCELLED') NOT NULL DEFAULT 'OPEN',

  opened_at BIGINT NULL,
  closed_at BIGINT NULL,

  notes VARCHAR(255) NULL,

  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_symbol_status (symbol, status),
  KEY idx_exchange_symbol_status (exchange, symbol, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`;

async function main() {
  logger.info('Running migration v3 (open_trades)...');
  await pool.query(sql);
  logger.info('Migration v3 done.');
  await pool.end();
}

main().catch(async (err) => {
  logger.error({ err }, 'Migration v3 failed');
  try { await pool.end(); } catch {}
  process.exit(1);
});
