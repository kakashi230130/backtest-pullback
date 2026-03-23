import { pool } from './db.js';

async function main() {
  const [candles] = await pool.query(
    `
    SELECT open_time, open, high, low, close, rsi, ma20, ma50, ma200
    FROM candles
    WHERE symbol='ETHUSDT' AND interval_code='5m'
    ORDER BY open_time DESC
    LIMIT 20
    `,
  );

  const [openTrades] = await pool.query(
    `
    SELECT id, symbol, side, status,
           entry_price, quantity, stop_loss, take_profit,
           entry_order_id, entry_order_status,
           sl_order_id, sl_order_status,
           tp_order_id, tp_order_status,
           opened_at, closed_at, last_sync_at, notes
    FROM open_trades
    ORDER BY id DESC
    LIMIT 10
    `,
  );

  console.log(JSON.stringify({ candles, openTrades }, null, 2));
  await pool.end();
}

main().catch(async (err) => {
  console.error(err);
  try { await pool.end(); } catch {}
  process.exit(1);
});
