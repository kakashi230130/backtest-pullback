import { pool } from './db.js';
import { logger } from './logger.js';
import { tradingEnabled } from './config.js';
import {
  getFuturesPremiumIndex,
  placeFuturesLimitOrder,
  placeFuturesStopLossMarket,
  placeFuturesTakeProfitMarket,
} from './binanceTradeClient.js';

function fmt(n) {
  return Number(n).toFixed(8);
}

async function main() {
  const symbol = (process.env.SAMPLE_SYMBOL ?? process.env.SYMBOLS ?? 'ETHUSDT').split(',')[0].trim();
  const qty = Number(process.env.SAMPLE_QTY ?? 0.01);
  const side = (process.env.SAMPLE_SIDE ?? 'BUY').toUpperCase(); // BUY or SELL
  const entryOffsetPct = Number(process.env.SAMPLE_ENTRY_OFFSET_PCT ?? 0.001); // 0.1%
  const slPct = Number(process.env.SAMPLE_SL_PCT ?? 0.003); // 0.3%
  const tpPct = Number(process.env.SAMPLE_TP_PCT ?? 0.006); // 0.6%

  if (!Number.isFinite(qty) || qty <= 0) throw new Error('SAMPLE_QTY must be > 0');
  if (!['BUY', 'SELL'].includes(side)) throw new Error('SAMPLE_SIDE must be BUY or SELL');

  const pi = await getFuturesPremiumIndex({ symbol });
  const mark = Number(pi.markPrice);
  if (!Number.isFinite(mark) || mark <= 0) throw new Error('Failed to get markPrice');

  const entry = side === 'BUY' ? mark * (1 - entryOffsetPct) : mark * (1 + entryOffsetPct);
  const sl = side === 'BUY' ? entry * (1 - slPct) : entry * (1 + slPct);
  const tp = side === 'BUY' ? entry * (1 + tpPct) : entry * (1 - tpPct);

  logger.info({ symbol, side, qty, mark, entry, sl, tp }, 'Placing SAMPLE orders');

  // Sample order script should only place ENTRY and insert DB.
  // SL/TP will be created by orderWatcher after entry is FILLED.
  const placeProtective = false;

  if (!tradingEnabled) {
    throw new Error('TRADING_ENABLED!=1. Refusing to place real sample order. Set TRADING_ENABLED=1 to proceed.');
  }

  // Use numbers here; binanceTradeClient will normalize to tickSize/stepSize.
  const clientId = `oc_sample_${Date.now()}`;
  const entryRes = await placeFuturesLimitOrder({ symbol, side, quantity: qty, price: entry, newClientOrderId: clientId });

  // Futures testnet may require Algo endpoints for STOP/TP orders.
  // Default: don't place SL/TP orders, rely on watcher markPrice failsafe.
  const positionSide = side === 'BUY' ? 'LONG' : 'SHORT';

  const slRes = placeProtective
    ? await placeFuturesStopLossMarket({
        symbol,
        side: side === 'BUY' ? 'SELL' : 'BUY',
        stopPrice: sl,
        quantity: qty,
        reduceOnly: null,
        positionSide,
      })
    : { orderId: null, status: null };

  const tpRes = placeProtective
    ? await placeFuturesTakeProfitMarket({
        symbol,
        side: side === 'BUY' ? 'SELL' : 'BUY',
        stopPrice: tp,
        quantity: qty,
        reduceOnly: null,
        positionSide,
      })
    : { orderId: null, status: null };

  const tradeSide = side === 'BUY' ? 'LONG' : 'SHORT';
  const now = Date.now();

  await pool.query(
    `
    INSERT INTO open_trades (
      exchange, symbol, side, leverage,
      entry_price, quantity, stop_loss, take_profit,
      status, opened_at, notes,
      entry_order_id, entry_client_order_id,
      sl_order_id, sl_client_order_id,
      tp_order_id, tp_client_order_id,
      entry_order_status, sl_order_status, tp_order_status,
      last_sync_at
    )
    VALUES (
      :exchange, :symbol, :tradeSide, NULL,
      :entry, :qty, :sl, :tp,
      'PENDING', :now, :notes,
      :entry_order_id, :entry_client_order_id,
      :sl_order_id, :sl_client_order_id,
      :tp_order_id, :tp_client_order_id,
      :entry_order_status, :sl_order_status, :tp_order_status,
      :now
    )
    `,
    {
      exchange: process.env.EXCHANGE_NAME ?? 'binance_usdtm_demo',
      symbol,
      tradeSide,
      entry: fmt(entry),
      qty: fmt(qty),
      sl: fmt(sl),
      tp: fmt(tp),
      now,
      notes: `SAMPLE_${side} mark=${mark}`.slice(0, 255),
      // orderId may exceed JS safe integer; rely on client id for watcher lookups.
      entry_order_id: null,
      entry_client_order_id: clientId,
      sl_order_id: null,
      sl_client_order_id: null,
      tp_order_id: null,
      tp_client_order_id: null,
      entry_order_status: entryRes.status ?? null,
      sl_order_status: null,
      tp_order_status: null,
    },
  );

  console.log(
    JSON.stringify(
      {
        symbol,
        side,
        qty,
        mark,
        entry,
        sl,
        tp,
        orders: {
          entry: { orderId: entryRes.orderId, status: entryRes.status },
          sl: { orderId: slRes.orderId, status: slRes.status },
          tp: { orderId: tpRes.orderId, status: tpRes.status },
        },
      },
      null,
      2,
    ),
  );

  if (process.env.CLOSE_DB_POOL === '1') {
    await pool.end();
  }
}

main().catch(async (err) => {
  const resp = err?.response;
  if (resp) {
    logger.error({
      status: resp.status,
      data: resp.data,
      headers: resp.headers,
      requestUrl: resp.config?.baseURL ? `${resp.config.baseURL}${resp.config.url}` : resp.config?.url,
    }, 'sampleOrder failed (HTTP)');
  } else {
    logger.error({ err }, 'sampleOrder failed');
  }
  try { await pool.end(); } catch {}
  process.exit(1);
});
