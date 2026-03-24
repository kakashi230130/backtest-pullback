import { pool } from './db.js';
import { logger } from './logger.js';
import { tradingEnabled, exchangeName, algoSltpEnabled, algoOrderPath } from './config.js';
import {
  getFuturesOrder,
  cancelFuturesOrder,
  getFuturesPremiumIndex,
  getFuturesPositionRisk,
  placeFuturesMarketOrder,
  placeFuturesStopLossMarket,
  placeFuturesTakeProfitMarket,
  placeFuturesStopLossLimit,
  placeFuturesTakeProfitLimit,
  placeFuturesAlgoOrder,
  placeFuturesAlgoConditional,
} from './binanceTradeClient.js';

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// Prevent spamming protective order creation within a single watcher burst run.
// Key: `${tradeId}:SL` / `${tradeId}:TP` / `${tradeId}:MOVE_SL` → lastAttemptMs
const protectiveAttemptMs = new Map();
function recentlyAttempted(key, windowMs) {
  const t = protectiveAttemptMs.get(key);
  return t != null && Date.now() - t < windowMs;
}
function markAttempt(key) {
  protectiveAttemptMs.set(key, Date.now());
}

// In-memory trailing state (persisted for the life of this watcher process).
// We avoid DB schema changes; restart will reset these, but SL position is still enforced on-exchange and via mark-price failsafe.
const trailState = new Map();
// tradeId -> { entry, initialSl, R, highWater, lowWater }

async function getTradesToSync(limit = 20) {
  const [rows] = await pool.query(
    `
    SELECT id, symbol, side, status,
           entry_price,
           quantity, stop_loss, take_profit,
           entry_order_id, entry_client_order_id,
           sl_order_id, sl_client_order_id,
           tp_order_id, tp_client_order_id
    FROM open_trades
    WHERE exchange = :exchange
      AND status IN ('PENDING','ACTIVE')
    ORDER BY id ASC
    LIMIT ${Number(limit) | 0}
    `,
    { exchange: exchangeName },
  );
  return rows.map(r => ({
    id: Number(r.id),
    symbol: r.symbol,
    side: r.side,
    status: r.status,
    entry_price: r.entry_price == null ? null : Number(r.entry_price),
    quantity: r.quantity == null ? null : Number(r.quantity),
    stop_loss: r.stop_loss == null ? null : Number(r.stop_loss),
    take_profit: r.take_profit == null ? null : Number(r.take_profit),
    // IMPORTANT: Binance orderId can exceed JS Number safe range. Keep as string.
    entry_order_id: r.entry_order_id == null ? null : String(r.entry_order_id),
    entry_client_order_id: r.entry_client_order_id ?? null,
    sl_order_id: r.sl_order_id == null ? null : String(r.sl_order_id),
    sl_client_order_id: r.sl_client_order_id ?? null,
    tp_order_id: r.tp_order_id == null ? null : String(r.tp_order_id),
    tp_client_order_id: r.tp_client_order_id ?? null,
  }));
}

async function updateTrade(id, patch) {
  const cols = Object.keys(patch);
  if (!cols.length) return;
  const sets = cols.map(c => `${c}=:${c}`).join(', ');
  await pool.query(`UPDATE open_trades SET ${sets} WHERE id=:id`, { id, ...patch });
}

async function cancelIfExists(symbol, orderId, origClientOrderId) {
  if (!orderId && !origClientOrderId) return;
  try {
    await cancelFuturesOrder({ symbol, orderId, origClientOrderId });
  } catch (err) {
    logger.warn({ err, symbol, orderId, origClientOrderId }, 'Cancel order failed (ignored)');
  }
}

function hitSlTpByPrice({ tradeSide, price, sl, tp }) {
  if (price == null) return { hit: null };
  if (tradeSide === 'LONG') {
    if (sl != null && price <= sl) return { hit: 'SL' };
    if (tp != null && price >= tp) return { hit: 'TP' };
  } else {
    if (sl != null && price >= sl) return { hit: 'SL' };
    if (tp != null && price <= tp) return { hit: 'TP' };
  }
  return { hit: null };
}

function clamp(n, lo, hi) {
  const x = Number(n);
  if (!Number.isFinite(x)) return lo;
  return Math.min(hi, Math.max(lo, x));
}

function roundToTick(x) {
  // We don't have exchange tickSize here; keep raw precision.
  // Binance will round/reject; if needed, implement symbol filters later.
  return Number(x);
}

async function placeOrUpdateStopLoss({ tr, stopPrice, now, reason }) {
  if (!tradingEnabled) return { placed: false, mode: 'DRY_RUN' };
  const closeSide = tr.side === 'LONG' ? 'SELL' : 'BUY';

  // 1) cancel old SL if any (best-effort)
  try { await cancelIfExists(tr.symbol, tr.sl_order_id, tr.sl_client_order_id); } catch {}

  // 2) place a fresh SL order (best-effort with fallbacks)
  let slRes = null;
  let slStatus = null;
  let slOrderId = null;
  let slClientId = null;

  try {
    try {
      slRes = await placeFuturesStopLossMarket({
        symbol: tr.symbol,
        side: closeSide,
        stopPrice: roundToTick(stopPrice),
        quantity: tr.quantity,
        closePosition: true,
        reduceOnly: null,
        positionSide: null,
      });
      slStatus = slRes?.status ?? 'NEW';
      slOrderId = slRes?.orderId ?? slRes?.algoId ?? null;
      slClientId = slRes?.clientOrderId ?? slRes?.clientAlgoId ?? null;
    } catch (err1) {
      const code = err1?.response?.data?.code;
      if (code === -4120) {
        // Fallback: STOP (limit) if STOP_MARKET unsupported on this account
        const stop = Number(stopPrice);
        const slipPct = Number(process.env.SL_LIMIT_SLIPPAGE_PCT ?? 0.001); // 0.1%
        const slip = clamp(slipPct, 0, 0.01);
        const limitPrice = closeSide === 'SELL' ? stop * (1 - slip) : stop * (1 + slip);

        try {
          slRes = await placeFuturesStopLossLimit({
            symbol: tr.symbol,
            side: closeSide,
            stopPrice: stop,
            price: limitPrice,
            quantity: tr.quantity,
            reduceOnly: true,
            positionSide: null,
          });
          slStatus = slRes?.status ?? 'NEW';
          slOrderId = slRes?.orderId ?? null;
          slClientId = slRes?.clientOrderId ?? null;
        } catch (err2) {
          const code2 = err2?.response?.data?.code;
          if (code2 === -4120) {
            logger.warn({ id: tr.id, symbol: tr.symbol, code: code2 }, 'Cannot place exchange SL (unsupported); will rely on mark-price failsafe');
            slRes = null;
            slStatus = 'UNSUPPORTED';
          } else {
            throw err2;
          }
        }
      } else {
        throw err1;
      }
    }
  } catch (err) {
    logger.warn({ err, id: tr.id, symbol: tr.symbol }, 'Move SL failed (ignored)');
  }

  await updateTrade(tr.id, {
    stop_loss: stopPrice,
    sl_order_id: slOrderId == null ? null : String(slOrderId),
    sl_client_order_id: slClientId ?? null,
    sl_order_status: slStatus,
    last_sync_at: now,
    notes: `${(reason ?? 'MOVE_SL')} @${stopPrice}`.slice(0, 255),
  });

  logger.info({ id: tr.id, symbol: tr.symbol, stopPrice, slStatus }, 'SL moved/updated');

  return { placed: slRes != null, slStatus };
}

async function closeNowAtMarket(tr, reason, price) {
  const now = Date.now();

  // 1) First: mark CLOSED in DB no matter what (so we don't miss TP/SL due to API issues)
  await pool.query(
    `UPDATE open_trades
     SET status='CLOSED', closed_at=:now, notes=:notes
     WHERE id=:id AND status='ACTIVE'`,
    { id: tr.id, now, notes: `${reason} @${price ?? 'NA'}`.slice(0, 255) },
  );

  // 2) Then: best-effort exchange cleanup/close (only if enabled)
  if (!tradingEnabled) {
    logger.warn({ id: tr.id, symbol: tr.symbol }, 'TRADING_ENABLED!=1, skipping exchange cancel/market-close');
    return;
  }

  try { await cancelIfExists(tr.symbol, tr.sl_order_id, tr.sl_client_order_id); } catch {}
  try { await cancelIfExists(tr.symbol, tr.tp_order_id, tr.tp_client_order_id); } catch {}
  try { await cancelIfExists(tr.symbol, tr.entry_order_id, tr.entry_client_order_id); } catch {}

  if (tr.quantity) {
    try {
      await placeFuturesMarketOrder({
        symbol: tr.symbol,
        side: tr.side === 'LONG' ? 'SELL' : 'BUY',
        // In One-way mode we must not send positionSide; trade client will strip it anyway.
        positionSide: null,
        quantity: tr.quantity,
        // Prefer reduceOnly for safety; if an account rejects it, binanceTradeClient only sends when not null.
        reduceOnly: true,
      });
    } catch (err) {
      logger.warn({ err, id: tr.id, symbol: tr.symbol }, 'Market close failed (ignored)');
    }
  }
}

async function syncOne(tr) {
  const now = Date.now();

  // If ACTIVE, also check current mark price against SL/TP (failsafe).
  if (tr.status === 'ACTIVE' && (tr.stop_loss != null || tr.take_profit != null)) {
    try {
      const pi = await getFuturesPremiumIndex({ symbol: tr.symbol });
      const price = Number(pi.markPrice);
      const { hit } = hitSlTpByPrice({ tradeSide: tr.side, price, sl: tr.stop_loss, tp: tr.take_profit });
      if (hit) {
        await closeNowAtMarket(tr, `HIT_${hit}_BY_PRICE`, price);
        return;
      }
    } catch (err) {
      logger.warn({ err, id: tr.id, symbol: tr.symbol }, 'Mark price SL/TP check failed (ignored)');
    }
  }

  if (tr.entry_order_id || tr.entry_client_order_id) {
    let entry;
    try {
      entry = await getFuturesOrder({
        symbol: tr.symbol,
        orderId: tr.entry_order_id,
        origClientOrderId: tr.entry_client_order_id,
      });
      await updateTrade(tr.id, { entry_order_status: entry.status, last_sync_at: now });
    } catch (err) {
      const code = err?.response?.data?.code;
      // If we can't look up the entry order (often due to orderId precision issues),
      // fall back to checking whether a position is open.
      if (code === -2013) {
        try {
          const pos = await getFuturesPositionRisk();
          const p = pos?.find(x => x.symbol === tr.symbol);
          const amt = p ? Number(p.positionAmt) : 0;
          if (amt !== 0) {
            logger.warn({ id: tr.id, symbol: tr.symbol, amt }, 'Entry order not found; position exists → mark ACTIVE');
            await updateTrade(tr.id, { status: 'ACTIVE', opened_at: now, entry_order_status: 'FILLED', last_sync_at: now });
            entry = { status: 'FILLED' };
          } else {
            throw err;
          }
        } catch (err2) {
          // If even the fallback fails, bubble original error.
          throw err;
        }
      } else {
        throw err;
      }
    }

    if (tr.status === 'PENDING') {
      if (entry.status === 'FILLED') {
        await updateTrade(tr.id, { status: 'ACTIVE', opened_at: now, entry_order_status: 'FILLED', last_sync_at: now });

        // After entry is filled, place protective SL/TP orders on-exchange (optional)
        const placeProtective = process.env.PLACE_SLTP_ORDERS === '1';
        if (placeProtective && tradingEnabled) {
          const closeSide = tr.side === 'LONG' ? 'SELL' : 'BUY';
          // SL
          try {
            if (tr.stop_loss != null && !tr.sl_order_id && !tr.sl_client_order_id) {
              let slRes;
              try {
                try {
                  slRes = await placeFuturesStopLossMarket({
                    symbol: tr.symbol,
                    side: closeSide,
                    stopPrice: tr.stop_loss,
                    quantity: tr.quantity,
                    closePosition: true,
                    reduceOnly: null,
                    positionSide: null,
                  });
                } catch (e2) {
                  const code2 = e2?.response?.data?.code;
                  if (code2 === -1106) {
                    // Some accounts reject reduceOnly on conditional orders.
                    logger.warn({ id: tr.id, symbol: tr.symbol, code: code2 }, 'SL reduceOnly rejected; retrying without reduceOnly');
                    slRes = await placeFuturesStopLossMarket({
                      symbol: tr.symbol,
                      side: closeSide,
                      stopPrice: tr.stop_loss,
                      quantity: tr.quantity,
                      closePosition: true,
                      reduceOnly: null,
                      positionSide: null,
                    });
                  } else {
                    throw e2;
                  }
                }
              } catch (err) {
                const code = err?.response?.data?.code;
                if (code === -4120 && algoSltpEnabled) {
                  // Some accounts require algo endpoints (but yours previously 404'd).
                  // Keep it behind the flag.
                  logger.warn({ id: tr.id, symbol: tr.symbol, code, algoOrderPath }, 'STOP_MARKET requires algo endpoint; retrying via algo');
                  slRes = await placeFuturesAlgoConditional({
                    algoOrderPath,
                    symbol: tr.symbol,
                    side: closeSide,
                    type: 'STOP_MARKET',
                    triggerPrice: tr.stop_loss,
                    quantity: tr.quantity,
                    reduceOnly: true,
                    workingType: 'MARK_PRICE',
                  });
                } else if (code === -4120) {
                  // Fallback: limit-style conditional order (STOP) with limit price slightly beyond stopPrice.
                  logger.warn({ id: tr.id, symbol: tr.symbol, code }, 'STOP_MARKET not supported; retrying with STOP (limit)');

                  const stop = Number(tr.stop_loss);
                  const slipPct = Number(process.env.SL_LIMIT_SLIPPAGE_PCT ?? 0.001); // 0.1%
                  const slip = Math.max(0, Math.min(slipPct, 0.01));
                  const limitPrice = closeSide === 'SELL' ? stop * (1 - slip) : stop * (1 + slip);

                  try {
                    slRes = await placeFuturesStopLossLimit({
                      symbol: tr.symbol,
                      side: closeSide,
                      stopPrice: stop,
                      price: limitPrice,
                      quantity: tr.quantity,
                      reduceOnly: true,
                      positionSide: null,
                    });
                  } catch (e3) {
                    const code3 = e3?.response?.data?.code;
                    if (code3 === -4120) {
                      // Some accounts require algo endpoint for ALL conditional order types.
                      // We fall back to local mark-price monitoring + market close.
                      logger.warn({ id: tr.id, symbol: tr.symbol, code: code3 }, 'STOP (limit) also unsupported; will rely on mark-price failsafe instead of on-exchange SL');
                      slRes = null;
                    } else {
                      throw e3;
                    }
                  }
                } else {
                  throw err;
                }
              }

              await updateTrade(tr.id, {
                sl_order_id: slRes == null ? null : String(slRes.orderId ?? slRes.algoId ?? null),
                sl_client_order_id: slRes?.clientOrderId ?? slRes?.clientAlgoId ?? null,
                sl_order_status:
                  slRes === null
                    ? 'UNSUPPORTED'
                    : slRes?.status ?? (slRes?.algoId != null ? 'ALGO_NEW' : null),
              });
            }
          } catch (err) {
            logger.warn({ err, id: tr.id, symbol: tr.symbol }, 'Place SL failed (ignored)');
          }

          // TP
          try {
            if (tr.take_profit != null && !tr.tp_order_id && !tr.tp_client_order_id) {
              let tpRes;
              try {
                try {
                  tpRes = await placeFuturesTakeProfitMarket({
                    symbol: tr.symbol,
                    side: closeSide,
                    stopPrice: tr.take_profit,
                    quantity: tr.quantity,
                    closePosition: true,
                    reduceOnly: null,
                    positionSide: null,
                  });
                } catch (e2) {
                  const code2 = e2?.response?.data?.code;
                  if (code2 === -1106) {
                    // Some accounts reject reduceOnly on conditional orders.
                    logger.warn({ id: tr.id, symbol: tr.symbol, code: code2 }, 'TP reduceOnly rejected; retrying without reduceOnly');
                    tpRes = await placeFuturesTakeProfitMarket({
                      symbol: tr.symbol,
                      side: closeSide,
                      stopPrice: tr.take_profit,
                      quantity: tr.quantity,
                      closePosition: true,
                      reduceOnly: null,
                      positionSide: null,
                    });
                  } else {
                    throw e2;
                  }
                }
              } catch (err) {
                const code = err?.response?.data?.code;
                if (code === -4120 && algoSltpEnabled) {
                  logger.warn({ id: tr.id, symbol: tr.symbol, code, algoOrderPath }, 'TAKE_PROFIT_MARKET requires algo endpoint; retrying via algo');
                  tpRes = await placeFuturesAlgoConditional({
                    algoOrderPath,
                    symbol: tr.symbol,
                    side: closeSide,
                    type: 'TAKE_PROFIT_MARKET',
                    triggerPrice: tr.take_profit,
                    quantity: tr.quantity,
                    reduceOnly: true,
                    workingType: 'MARK_PRICE',
                  });
                } else if (code === -4120) {
                  // Fallback: limit-style conditional order (TAKE_PROFIT) with limit price slightly inside stopPrice.
                  logger.warn({ id: tr.id, symbol: tr.symbol, code }, 'TAKE_PROFIT_MARKET not supported; retrying with TAKE_PROFIT (limit)');

                  const stop = Number(tr.take_profit);
                  const slipPct = Number(process.env.TP_LIMIT_SLIPPAGE_PCT ?? 0.001); // 0.1%
                  const slip = Math.max(0, Math.min(slipPct, 0.01));
                  const limitPrice = closeSide === 'SELL' ? stop * (1 - slip) : stop * (1 + slip);

                  try {
                    tpRes = await placeFuturesTakeProfitLimit({
                      symbol: tr.symbol,
                      side: closeSide,
                      stopPrice: stop,
                      price: limitPrice,
                      quantity: tr.quantity,
                      reduceOnly: true,
                      positionSide: null,
                    });
                  } catch (e3) {
                    const code3 = e3?.response?.data?.code;
                    if (code3 === -4120) {
                      logger.warn({ id: tr.id, symbol: tr.symbol, code: code3 }, 'TAKE_PROFIT (limit) also unsupported; will rely on mark-price failsafe instead of on-exchange TP');
                      tpRes = null;
                    } else {
                      throw e3;
                    }
                  }
                } else {
                  throw err;
                }
              }

              await updateTrade(tr.id, {
                tp_order_id: tpRes == null ? null : String(tpRes.orderId ?? tpRes.algoId ?? null),
                tp_client_order_id: tpRes?.clientOrderId ?? tpRes?.clientAlgoId ?? null,
                tp_order_status:
                  tpRes === null
                    ? 'UNSUPPORTED'
                    : tpRes?.status ?? (tpRes?.algoId != null ? 'ALGO_NEW' : null),
              });
            }
          } catch (err) {
            logger.warn({ err, id: tr.id, symbol: tr.symbol }, 'Place TP failed (ignored)');
          }
        }

        // IMPORTANT: stop here so we don't also run ACTIVE logic in the same tick.
        return;
      }
      if (['CANCELED', 'REJECTED', 'EXPIRED'].includes(entry.status)) {
        await updateTrade(tr.id, { status: 'CANCELLED', closed_at: now, notes: `ENTRY_${entry.status}`.slice(0,255) });
        await cancelIfExists(tr.symbol, tr.sl_order_id);
        await cancelIfExists(tr.symbol, tr.tp_order_id);
        return;
      }
    }
  }

  if (tr.status === 'ACTIVE') {
    // Auto breakeven + trailing stop (optional)
    // - Default behavior:
    //   + at +1R: move SL to entry (optionally with small offset)
    //   + from +2R: trail in 1R steps (lock 1R at 2R, lock 2R at 3R, ...)
    const beEnabled = (process.env.AUTO_BREAKEVEN_ENABLED ?? '1') === '1';
    const trailEnabled = (process.env.TRAILING_STOP_ENABLED ?? '1') === '1';

    if ((beEnabled || trailEnabled) && tr.entry_price != null && tr.stop_loss != null && tr.quantity) {
      const entry = Number(tr.entry_price);
      const slNow = Number(tr.stop_loss);

      if (Number.isFinite(entry) && Number.isFinite(slNow)) {
        // initialize state (watermarks) for this trade id
        if (!trailState.has(tr.id)) {
          const R0 = tr.side === 'LONG' ? (entry - slNow) : (slNow - entry);
          if (!Number.isFinite(R0) || R0 <= 0) {
            // can't compute R, skip
            // (this can happen if SL got set on the wrong side due to bad data)
            return;
          }
          trailState.set(tr.id, {
            entry,
            initialSl: slNow,
            R: R0,
            highWater: null,
            lowWater: null,
          });
        }

        try {
          const pi = await getFuturesPremiumIndex({ symbol: tr.symbol });
          const price = Number(pi.markPrice);

          if (Number.isFinite(price) && price > 0) {
            const stW = trailState.get(tr.id);
            if (stW) {
              stW.highWater = stW.highWater == null ? price : Math.max(stW.highWater, price);
              stW.lowWater = stW.lowWater == null ? price : Math.min(stW.lowWater, price);
            }

            const Rbase = Number(stW?.R);
            if (!Number.isFinite(Rbase) || Rbase <= 0) return;

            const favorable = tr.side === 'LONG' ? (price - entry) : (entry - price);
            const favorableR = favorable / Rbase;

            const moveWindowMs = Number(process.env.MOVE_SL_RETRY_WINDOW_MS ?? 30000);
            const attemptKey = `${tr.id}:MOVE_SL`;

            // 1) Breakeven at +1R (only if SL is still on the risky side of entry)
            if (beEnabled && favorableR >= Number(process.env.AUTO_BE_AT_R ?? 1)) {
              const beOffsetPct = Number(process.env.AUTO_BE_OFFSET_PCT ?? 0); // e.g. 0.0001 = 0.01%
              const off = entry * clamp(beOffsetPct, 0, 0.002);
              const bePrice = tr.side === 'LONG' ? (entry + off) : (entry - off);

              const needsBE = tr.side === 'LONG' ? slNow < bePrice : slNow > bePrice;
              if (needsBE && !recentlyAttempted(attemptKey, moveWindowMs)) {
                markAttempt(attemptKey);
                await placeOrUpdateStopLoss({ tr, stopPrice: bePrice, now, reason: 'AUTO_BREAKEVEN' });
                return; // avoid doing more actions in same tick after moving SL
              }
            }

            // 2) Trailing from +2R: lock profits in 1R steps
            if (trailEnabled && favorableR >= Number(process.env.TRAIL_START_R ?? 2)) {
              const lockR = Math.max(1, Math.floor(favorableR) - 1);
              const desired = tr.side === 'LONG'
                ? (entry + lockR * Rbase)
                : (entry - lockR * Rbase);

              const needsTrail = tr.side === 'LONG' ? slNow < desired : slNow > desired;
              if (needsTrail && !recentlyAttempted(attemptKey, moveWindowMs)) {
                markAttempt(attemptKey);
                await placeOrUpdateStopLoss({ tr, stopPrice: desired, now, reason: `AUTO_TRAIL_LOCK_${lockR}R` });
                return;
              }
            }
          }
        } catch (err) {
          logger.warn({ err, id: tr.id, symbol: tr.symbol }, 'Breakeven/trailing check failed (ignored)');
        }
      }
    }

    // By default, do NOT create new SL/TP when trade is already ACTIVE.
    // Set ENABLE_ACTIVE_BACKFILL=1 only if you explicitly want the watcher to place missing protective orders.
    const enableActiveBackfill = process.env.ENABLE_ACTIVE_BACKFILL === '1';
    const placeProtective = process.env.PLACE_SLTP_ORDERS === '1';
    if (enableActiveBackfill && placeProtective && tradingEnabled) {
      const closeSide = tr.side === 'LONG' ? 'SELL' : 'BUY';

      if (tr.stop_loss != null && !tr.sl_order_id && !tr.sl_client_order_id) {
        const attemptKey = `${tr.id}:SL`;
        if (recentlyAttempted(attemptKey, Number(process.env.SLTP_RETRY_WINDOW_MS ?? 120000))) {
          logger.warn({ id: tr.id, symbol: tr.symbol }, 'Skip SL backfill (recently attempted)');
        } else {
          markAttempt(attemptKey);
          try {
            let slRes;
            let usedAlgo = false;
            try {
              slRes = await placeFuturesStopLossMarket({
                symbol: tr.symbol,
                side: closeSide,
                stopPrice: tr.stop_loss,
                quantity: tr.quantity,
                closePosition: true,
                reduceOnly: null,
                positionSide: null,
              });
            } catch (err1) {
              const code = err1?.response?.data?.code;
              if (code === -4120 && algoSltpEnabled) {
                usedAlgo = true;
                logger.warn({ id: tr.id, symbol: tr.symbol, code, algoOrderPath }, 'Backfill SL requires algo endpoint; retrying via algo');
                slRes = await placeFuturesAlgoConditional({
                  algoOrderPath,
                  symbol: tr.symbol,
                  side: closeSide,
                  type: 'STOP_MARKET',
                  triggerPrice: tr.stop_loss,
                  quantity: tr.quantity,
                  reduceOnly: true,
                  workingType: 'MARK_PRICE',
                });
              } else {
                throw err1;
              }
            }

            logger.info({ id: tr.id, symbol: tr.symbol, usedAlgo, slRes }, 'Placed SL');

            await updateTrade(tr.id, {
              sl_order_id: slRes == null ? null : String(slRes.orderId ?? slRes.algoId ?? slRes.algoOrderId ?? null),
              sl_client_order_id: slRes?.clientOrderId ?? slRes?.clientAlgoId ?? null,
              // Mark algo placements explicitly so we don't try to sync them via /fapi/v1/order.
              sl_order_status: usedAlgo ? 'ALGO_PLACED' : (slRes?.status ?? 'NEW'),
              last_sync_at: now,
            });
          } catch (err) {
            logger.warn({ err, id: tr.id, symbol: tr.symbol }, 'Backfill SL failed (ignored)');
          }
        }
      }

      if (tr.take_profit != null && !tr.tp_order_id && !tr.tp_client_order_id) {
        const attemptKey = `${tr.id}:TP`;
        if (recentlyAttempted(attemptKey, Number(process.env.SLTP_RETRY_WINDOW_MS ?? 120000))) {
          logger.warn({ id: tr.id, symbol: tr.symbol }, 'Skip TP backfill (recently attempted)');
        } else {
          markAttempt(attemptKey);
          try {
            let tpRes;
            let usedAlgo = false;
            try {
              tpRes = await placeFuturesTakeProfitMarket({
                symbol: tr.symbol,
                side: closeSide,
                stopPrice: tr.take_profit,
                quantity: tr.quantity,
                closePosition: true,
                reduceOnly: null,
                positionSide: null,
              });
            } catch (err1) {
              const code = err1?.response?.data?.code;
              if (code === -4120 && algoSltpEnabled) {
                usedAlgo = true;
                logger.warn({ id: tr.id, symbol: tr.symbol, code, algoOrderPath }, 'Backfill TP requires algo endpoint; retrying via algo');
                tpRes = await placeFuturesAlgoConditional({
                  algoOrderPath,
                  symbol: tr.symbol,
                  side: closeSide,
                  type: 'TAKE_PROFIT_MARKET',
                  triggerPrice: tr.take_profit,
                  quantity: tr.quantity,
                  reduceOnly: true,
                  workingType: 'MARK_PRICE',
                });
              } else {
                throw err1;
              }
            }

            logger.info({ id: tr.id, symbol: tr.symbol, usedAlgo, tpRes }, 'Placed TP');

            await updateTrade(tr.id, {
              tp_order_id: tpRes == null ? null : String(tpRes.orderId ?? tpRes.algoId ?? tpRes.algoOrderId ?? null),
              tp_client_order_id: tpRes?.clientOrderId ?? tpRes?.clientAlgoId ?? null,
              tp_order_status: usedAlgo ? 'ALGO_PLACED' : (tpRes?.status ?? 'NEW'),
              last_sync_at: now,
            });
          } catch (err) {
            logger.warn({ err, id: tr.id, symbol: tr.symbol }, 'Backfill TP failed (ignored)');
          }
        }
      }
    }

    // If SL/TP were created via Algo Service, they are NOT queryable via /fapi/v1/order.
    // Skip querying them here to avoid repeated -2013 errors.
    if ((tr.sl_order_id || tr.sl_client_order_id) && !String(tr.sl_order_status ?? '').startsWith('ALGO')) {
      try {
        const sl = await getFuturesOrder({
          symbol: tr.symbol,
          orderId: tr.sl_order_id,
          origClientOrderId: tr.sl_client_order_id,
        });
        await updateTrade(tr.id, { sl_order_status: sl.status, last_sync_at: now });
        if (sl.status === 'FILLED') {
          await updateTrade(tr.id, { status: 'CLOSED', closed_at: now, notes: 'SL_FILLED' });
          await cancelIfExists(tr.symbol, tr.tp_order_id);
          return;
        }
      } catch (err) {
        const code = err?.response?.data?.code;
        if (code === -2013) {
          // Stale IDs (order never existed or already canceled). Clear so backfill can try again.
          logger.warn({ id: tr.id, symbol: tr.symbol }, 'SL order not found; clearing SL ids');
          await updateTrade(tr.id, {
            sl_order_id: null,
            sl_client_order_id: null,
            sl_order_status: 'MISSING',
            last_sync_at: now,
          });
        } else {
          throw err;
        }
      }
    }

    if ((tr.tp_order_id || tr.tp_client_order_id) && !String(tr.tp_order_status ?? '').startsWith('ALGO')) {
      try {
        const tp = await getFuturesOrder({
          symbol: tr.symbol,
          orderId: tr.tp_order_id,
          origClientOrderId: tr.tp_client_order_id,
        });
        await updateTrade(tr.id, { tp_order_status: tp.status, last_sync_at: now });
        if (tp.status === 'FILLED') {
          await updateTrade(tr.id, { status: 'CLOSED', closed_at: now, notes: 'TP_FILLED' });
          await cancelIfExists(tr.symbol, tr.sl_order_id);
          return;
        }
      } catch (err) {
        const code = err?.response?.data?.code;
        if (code === -2013) {
          logger.warn({ id: tr.id, symbol: tr.symbol }, 'TP order not found; clearing TP ids');
          await updateTrade(tr.id, {
            tp_order_id: null,
            tp_client_order_id: null,
            tp_order_status: 'MISSING',
            last_sync_at: now,
          });
        } else {
          throw err;
        }
      }
    }
  }
}

async function main() {
  const tickMs = Number(process.env.ORDER_WATCH_MS ?? 2000);

  // NOTE: Despite the filename, this watcher is intended to run as a long-lived daemon under PM2.
  // It continuously syncs PENDING/ACTIVE trades, and (optionally) places protective SL/TP after entry FILLED.
  // If you want a one-shot/burst run, set ORDER_WATCH_RUN_SECONDS to a positive number.
  const runSeconds = Number(process.env.ORDER_WATCH_RUN_SECONDS ?? 0);
  const until = runSeconds > 0 ? Date.now() + runSeconds * 1000 : null;

  logger.info({ tickMs, runSeconds }, 'Order watcher started');

  // Keep running forever (or until runSeconds elapses)
  while (until == null || Date.now() < until) {
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

    await sleep(tickMs);
  }

  if (process.env.CLOSE_DB_POOL === '1') {
    await pool.end();
  }
}

main().catch(async (err) => {
  logger.error({ err }, 'Order watcher failed');
  try { await pool.end(); } catch {}
  process.exit(1);
});
