import mysql from 'mysql2/promise';
import { config } from './config.js';

export const pool = mysql.createPool({
  host: config.db.host,
  port: config.db.port,
  user: config.db.user,
  password: config.db.password,
  database: config.db.database,
  waitForConnections: true,
  // You likely run multiple long-lived Node processes (one per interval/job).
  // Keep this modest to avoid exhausting MySQL max_connections.
  connectionLimit: Number(process.env.DB_POOL_LIMIT ?? 3),
  namedPlaceholders: true,
  timezone: 'Z',

  // IMPORTANT: orderId / BIGINT values can exceed JS safe integer range.
  // Always return big numbers as strings so we don't corrupt Binance orderIds.
  supportBigNumbers: true,
  bigNumberStrings: true,
});

export async function pingDb() {
  const conn = await pool.getConnection();
  try {
    await conn.ping();
  } finally {
    conn.release();
  }
}
