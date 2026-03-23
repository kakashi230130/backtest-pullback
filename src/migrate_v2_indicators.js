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
  logger.info('Running migration v2 (indicators)...');

  await addColumn('candles', 'rsi', 'rsi DECIMAL(20, 8) NULL');
  await addColumn('candles', 'ma20', 'ma20 DECIMAL(30, 12) NULL');
  await addColumn('candles', 'ma50', 'ma50 DECIMAL(30, 12) NULL');
  await addColumn('candles', 'ma200', 'ma200 DECIMAL(30, 12) NULL');

  logger.info('Migration v2 done.');
  await pool.end();
}

main().catch(async (err) => {
  logger.error({ err }, 'Migration v2 failed');
  try { await pool.end(); } catch {}
  process.exit(1);
});
