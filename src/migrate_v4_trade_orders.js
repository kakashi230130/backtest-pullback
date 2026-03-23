import { pool } from './db.js';
import { logger } from './logger.js';

async function columnExists(table, column) {
  const [rows] = await pool.query(
    `SELECT COUNT(*) AS cnt
     FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME = :table
       AND COLUMN_NAME = :column`,
    { table, column },
  );
  return Number(rows[0].cnt) > 0;
}

async function addColumn(table, column, ddl) {
  const exists = await columnExists(table, column);
  if (exists) {
    logger.info({ table, column }, 'Column exists, skip');
    return;
  }
  logger.info({ table, column }, 'Adding column');
  await pool.query(`ALTER TABLE ${table} ADD COLUMN ${ddl}`);
}

async function main() {
  logger.info('Running migration v4 (trade order tracking)...');

  // Expand trade lifecycle states
  logger.info('Updating open_trades.status enum...');
  await pool.query(
    `ALTER TABLE open_trades
     MODIFY COLUMN status ENUM('PENDING','ACTIVE','CLOSED','CANCELLED','REJECTED') NOT NULL DEFAULT 'PENDING'`,
  );

  // Entry / protective orders
  await addColumn('open_trades', 'entry_order_id', 'entry_order_id BIGINT NULL');
  await addColumn('open_trades', 'entry_client_order_id', 'entry_client_order_id VARCHAR(64) NULL');

  await addColumn('open_trades', 'sl_order_id', 'sl_order_id BIGINT NULL');
  await addColumn('open_trades', 'sl_client_order_id', 'sl_client_order_id VARCHAR(64) NULL');

  await addColumn('open_trades', 'tp_order_id', 'tp_order_id BIGINT NULL');
  await addColumn('open_trades', 'tp_client_order_id', 'tp_client_order_id VARCHAR(64) NULL');

  await addColumn('open_trades', 'entry_order_status', "entry_order_status VARCHAR(32) NULL");
  await addColumn('open_trades', 'sl_order_status', "sl_order_status VARCHAR(32) NULL");
  await addColumn('open_trades', 'tp_order_status', "tp_order_status VARCHAR(32) NULL");

  await addColumn('open_trades', 'last_sync_at', 'last_sync_at BIGINT NULL');

  logger.info('Migration v4 done.');
  await pool.end();
}

main().catch(async (err) => {
  logger.error({ err }, 'Migration v4 failed');
  try { await pool.end(); } catch {}
  process.exit(1);
});
