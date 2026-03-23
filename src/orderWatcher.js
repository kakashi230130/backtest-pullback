import { pool } from './db.js';
import { logger } from './logger.js';
import {
  getFuturesOrder,
  cancelFuturesOrder,
  placeFuturesMarketOrder,
  getFuturesPositionRisk,
} from './binanceTradeClient.js';

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function oppSide(tradeSide) {
  return tradeSide === 'LONG' ? 'SELL' : 'BUY';
}

async function getTradesToSync(limit = 20) {
  const [rows] = await pool.query(
    `
    SELECT *
    FROM open_trades
    WHERE status IN ('PENDING','ACTIVE')
    ORDER BY id ASC
    LIMIT ${Number(limit) | 0}
    `,
  );
  return rows.map(r => ({
    id: Number(r.id),
    exchange: r.exchange,
    symbol: r.symbol,
    side: r.side,
    leverage: r.leverage == null ? null : Number(r.leverage),
    entry_price: Number(r.entry_price),
    quantity: Number(r.quantity),
    stop_loss: r.stop_loss == null ? null : Number(r.stop_loss),
    take_profit: r.take_profit == null ? null : Number(r.take_profit),

    // IMPORTANT: Binance orderId can exceed JS Number safe range. Keep as string.
    entry_order_id: r.entry_order_id == null ? null : String(r.entry_order_id),
    sl_order_id: r.sl_order_id == null ? null : String(r.sl_order_id),
    tp_order_id: r.tp_order_id == null ? null : String(r.tp_order_id),

    status: r.status,
  }));
}

async function updateTrade(id, patch) {
  const cols = Object.keys(patch);
  if (!cols.length) return;
  const sets = cols.map(c => `${c}=:${c}`).join(', ');
  await pool.query(
    `UPDATE open_trades SET ${sets} WHERE id=:id`,
    { id, ...patch },
  );
}

async function markClosedBy(id, reason) {
  await updateTrade(id, {
    status: 'CLOSED',
    closed_at: Date.now(),
    notes: String(reason).slice(0, 255),
  });
}

async function markCancelled(id, reason) {
  await updateTrade(id, {
    status: 'CANCELLED',
    closed_at: Date.now(),
    notes: String(reason).slice(0, 255),
  });
}

async function cancelIfExists(symbol, orderId) {
  if (!orderId) return;
  try {
    await cancelFuturesOrder({ symbol, orderId });
  } catch (err) {
    // If already canceled/filled, Binance returns error; we keep going.
    logger.warn({ err, symbol, orderId }, 'Cancel order failed (ignored)');
  }
}

async function closePositionIfAny({ symbol, tradeSide }) {
  const positions = await getFuturesPositionRisk();
  const p = positions.find(x => x.symbol === symbol);
  if (!p) return { closed: false, reason: 'NO_POSITION' };

  // For BOTH mode, positionAmt >0 long, <0 short
  const amt = Number(p.positionAmt);
  if (!Number.isFinite(amt) || amt === 0) return { closed: false, reason: 'ZERO_POSITION' };

  const side = amt > 0 ? 'SELL' : 'BUY';
  const qty = Math.abs(amt);

  await placeFuturesMarketOrder({ symbol, side, quantity: qty });
  return { closed: true, qty, side };
}

async function syncOne(tr) {
  const now = Date.now();

  // 1) Sync entry order (if any)
  let entry = null;
  if (tr.entry_order_id) {
    entry = await getFuturesOrder({ symbol: tr.symbol, orderId: tr.entry_order_id });
    await updateTrade(tr.id, {
      entry_order_status: entry.status,
      last_sync_at: now,
    });

    if (tr.status === 'PENDING') {
      if (entry.status === 'FILLED') {
        await updateTrade(tr.id, { status: 'ACTIVE', opened_at: now });
      }
      if (['CANCELED', 'REJECTED', 'EXPIRED'].includes(entry.status)) {
        await markCancelled(tr.id, `ENTRY_${entry.status}`);
        // best-effort cleanup
        await cancelIfExists(tr.symbol, tr.sl_order_id);
        await cancelIfExists(tr.symbol, tr.tp_order_id);
        return;
      }
    }
  }

  // 2) If ACTIVE, see if SL/TP filled
  if (tr.status === 'ACTIVE') {
    let sl = null;
    let tp = null;

    if (tr.sl_order_id) {
      sl = await getFuturesOrder({ symbol: tr.symbol, orderId: tr.sl_order_id });
      await updateTrade(tr.id, { sl_order_status: sl.status, last_sync_at: now });
    }
    if (tr.tp_order_id) {
      tp = await getFuturesOrder({ symbol: tr.symbol, orderId: tr.tp_order_id });
      await updateTrade(tr.id, { tp_order_status: tp.status, last_sync_at: now });
    }

    if (sl && sl.status === 'FILLED') {
      await markClosedBy(tr.id, 'SL_FILLED');
      await cancelIfExists(tr.symbol, tr.tp_order_id);
      return;
    }
    if (tp && tp.status === 'FILLED') {
      await markClosedBy(tr.id, 'TP_FILLED');
      await cancelIfExists(tr.symbol, tr.sl_order_id);
      return;
    }
  }
}

async function main() {
  const intervalMs = Number(process.env.ORDER_WATCH_MS ?? 2000);
  logger.info({ intervalMs }, 'Order watcher started');

  // Keep running forever
  // NOTE: This is a long-lived process; do not set CLOSE_DB_POOL=1.
  while (true) {
    try {
      const trades = await getTradesToSync(20);
      for (const tr of trades) {
        try {
          await syncOne(tr);
        } catch (err) {
          logger.error({ err, id: tr.id, symbol: tr.symbol }, 'Sync trade failed');
        }
      }
    } catch (err) {
      logger.error({ err }, 'Watcher loop failed');
    }

    await sleep(intervalMs);
  }
}

main().catch((err) => {
  logger.error({ err }, 'Order watcher fatal');
  process.exit(1);
});
