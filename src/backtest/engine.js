import { addIndicatorsToCandleRows } from '../indicators.js';
import { INTERVALS, analyzeSymbolFromCandles } from '../strategy/analyzeCore.js';
import { maybeMoveStopLoss } from '../strategy/trailingCore.js';

function msForInterval(interval) {
  switch (interval) {
    case '5m': return 5 * 60 * 1000;
    case '15m': return 15 * 60 * 1000;
    case '30m': return 30 * 60 * 1000;
    case '1h': return 60 * 60 * 1000;
    case '4h': return 4 * 60 * 60 * 1000;
    case '1d': return 24 * 60 * 60 * 1000;
    default: throw new Error(`Unknown interval: ${interval}`);
  }
}

function clamp(x, lo, hi) {
  const n = Number(x);
  if (!Number.isFinite(n)) return lo;
  return Math.min(hi, Math.max(lo, n));
}

function sideFromAction(action) {
  return action === 'BUY' ? 'LONG' : 'SHORT';
}

function candleHitsPrice(c, price) {
  if (!c || price == null) return false;
  return Number(c.low) <= Number(price) && Number(c.high) >= Number(price);
}

function applySlippage({ side, price, slippagePct }) {
  const p = Number(price);
  const s = clamp(slippagePct, 0, 0.01);
  if (!(p > 0) || !(s > 0)) return p;
  // worse fill:
  // - LONG entry/exit buy: higher
  // - SHORT entry sell: lower? Actually short entry is SELL, so worse is lower price? For sells, worse is lower.
  // We'll interpret as: adverse to trader.
  if (side === 'LONG') return p * (1 + s);
  return p * (1 - s);
}

function calcFee({ notional, feeRate }) {
  const f = clamp(feeRate, 0, 0.01);
  return Math.abs(Number(notional) || 0) * f;
}

function pnlLinearUSDT({ side, entry, exit, qty }) {
  const e = Number(entry);
  const x = Number(exit);
  const q = Number(qty);
  if (![e, x, q].every(Number.isFinite)) return 0;
  if (side === 'LONG') return (x - e) * q;
  return (e - x) * q;
}

function computeMaxDrawdown(equityCurve) {
  let peak = -Infinity;
  let maxDD = 0;
  for (const pt of equityCurve) {
    const e = pt.equity;
    if (!Number.isFinite(e)) continue;
    if (e > peak) peak = e;
    const dd = peak > 0 ? (peak - e) / peak : 0;
    if (dd > maxDD) maxDD = dd;
  }
  return maxDD;
}

export function buildBacktestSummary({ trades, equityCurve, initialBalance }) {
  const total = trades.length;
  const wins = trades.filter(t => t.netPnl > 0).length;
  const losses = trades.filter(t => t.netPnl < 0).length;
  const winRate = total ? wins / total : 0;
  const netProfit = trades.reduce((a, t) => a + t.netPnl, 0);
  const grossProfit = trades.filter(t => t.netPnl > 0).reduce((a, t) => a + t.netPnl, 0);
  const grossLoss = trades.filter(t => t.netPnl < 0).reduce((a, t) => a + Math.abs(t.netPnl), 0);
  const profitFactor = grossLoss > 0 ? grossProfit / grossLoss : null;
  const maxDrawdown = computeMaxDrawdown(equityCurve);

  const finalEquity = equityCurve.length ? equityCurve[equityCurve.length - 1].equity : initialBalance;

  return {
    initial_balance: initialBalance,
    final_equity: finalEquity,
    net_profit: netProfit,
    total_trades: total,
    wins,
    losses,
    win_rate: winRate,
    profit_factor: profitFactor,
    max_drawdown: maxDrawdown,
  };
}

export function runBacktest({
  symbol,
  startTime,
  endTime,
  initialBalance = 1000,
  riskPerTrade = 0.01,
  leverage = 10,
  feeRate = 0.0004,
  slippagePct = 0,
  data, // { '5m': [...], '15m': [...], ... }
  indicatorsFromDb = true,
}) {
  if (!symbol) throw new Error('symbol required');

  // Ensure indicators exist; if DB has them, good. If not, compute in-memory.
  if (!indicatorsFromDb) {
    for (const itv of Object.keys(data)) {
      addIndicatorsToCandleRows(data[itv]);
    }
  }

  const mainTf = '5m';
  const candles5 = data[mainTf] ?? [];
  const start = Number(startTime);
  const end = Number(endTime);

  // Time pointers for each timeframe
  const ptr = {};
  for (const itv of INTERVALS) ptr[itv] = 0;

  // Find starting index in 5m data
  let i0 = candles5.findIndex(c => c.open_time >= start);
  if (i0 < 0) i0 = candles5.length;

  let equity = Number(initialBalance);
  let balance = Number(initialBalance);

  const equityCurve = [];
  const trades = [];

  let open = null; // current position/trade state
  let pending = null; // pending limit entry state

  const stepMs = msForInterval('5m');

  for (let i = i0; i < candles5.length; i++) {
    const c5 = candles5[i];
    if (c5.open_time > end) break;

    const nowMs = c5.open_time + stepMs; // treat as candle close time for staleness checks

    // Advance pointers for each tf: include candles with open_time <= current 5m open_time
    const snapData = {};
    for (const itv of INTERVALS) {
      const arr = data[itv] ?? [];
      let p = ptr[itv];
      while (p < arr.length && arr[p].open_time <= c5.open_time) p++;
      ptr[itv] = p;
      snapData[itv] = arr.slice(0, p);
    }

    // 1) If pending entry, check fill on this candle (option B)
    if (pending) {
      const filled = candleHitsPrice(c5, pending.entryPrice);
      if (filled) {
        const entryFill = applySlippage({ side: pending.side, price: pending.entryPrice, slippagePct });
        const notional = entryFill * pending.qty;
        const feeIn = calcFee({ notional, feeRate });
        balance -= feeIn;
        equity = balance;

        open = {
          ...pending,
          entryFill,
          entryTime: c5.open_time,
          initialSl: pending.sl,
          currentSl: pending.sl,
          tp: pending.tp,
          fees: feeIn,
          maxFavorable: entryFill,
          minFavorable: entryFill,
        };
        pending = null;
      }
    }

    // 2) If position open: check SL/TP hit on this candle; conservative SL-first if both
    if (open) {
      // Update trailing/breakeven based on close price (proxy for mark price)
      const mv = maybeMoveStopLoss({
        side: open.side,
        entry: open.entryFill,
        initialSl: open.initialSl,
        currentSl: open.currentSl,
        price: c5.close,
      });
      if (mv.newSl != null) {
        // tighten only
        if (open.side === 'LONG') open.currentSl = Math.max(open.currentSl, mv.newSl);
        else open.currentSl = Math.min(open.currentSl, mv.newSl);
      }

      const hitSL = open.side === 'LONG'
        ? (c5.low <= open.currentSl)
        : (c5.high >= open.currentSl);
      const hitTP = open.side === 'LONG'
        ? (c5.high >= open.tp)
        : (c5.low <= open.tp);

      let exitReason = null;
      let exitPrice = null;

      if (hitSL || hitTP) {
        if (hitSL && hitTP) {
          // conservative
          exitReason = 'SL';
          exitPrice = open.currentSl;
        } else if (hitSL) {
          exitReason = 'SL';
          exitPrice = open.currentSl;
        } else {
          exitReason = 'TP';
          exitPrice = open.tp;
        }

        const exitFill = applySlippage({ side: open.side, price: exitPrice, slippagePct });
        const notionalOut = exitFill * open.qty;
        const feeOut = calcFee({ notional: notionalOut, feeRate });

        const gross = pnlLinearUSDT({ side: open.side, entry: open.entryFill, exit: exitFill, qty: open.qty });
        const net = gross - open.fees - feeOut;

        balance += net;
        equity = balance;

        trades.push({
          symbol,
          side: open.side,
          entry_time: open.entryTime,
          exit_time: c5.open_time,
          entry: open.entryFill,
          exit: exitFill,
          qty: open.qty,
          sl_initial: open.initialSl,
          sl_final: open.currentSl,
          tp: open.tp,
          reason: exitReason,
          grossPnl: gross,
          fees: open.fees + feeOut,
          netPnl: net,
          leverage,
          margin_used: (open.entryFill * open.qty) / Math.max(1, Number(leverage) || 1),
          meta: open.meta ?? null,
        });

        open = null;
      }
    }

    // 3) If no open and no pending: run strategy at this time and possibly place LIMIT entry
    if (!open && !pending) {
      const analysis = analyzeSymbolFromCandles({ symbol, data: snapData, nowMs });
      const setup = analysis?.setup;

      if (setup && (setup.action === 'BUY' || setup.action === 'SELL')) {
        const side = sideFromAction(setup.action);

        const entry = Number(setup.entry);
        const sl = Number(setup.sl);
        const tp = Number(setup.tp);

        const stopDist = Math.abs(entry - sl);
        if (stopDist > 0 && Number.isFinite(stopDist)) {
          const risk$ = balance * clamp(riskPerTrade, 0, 1);
          const qty = risk$ / stopDist;

          // Margin check (leverage affects margin only). If insufficient balance, skip.
          const notional = entry * qty;
          const marginNeed = notional / Math.max(1, Number(leverage) || 1);
          if (marginNeed <= balance && qty > 0) {
            pending = {
              side,
              entryPrice: entry,
              sl,
              tp,
              qty,
              placedTime: c5.open_time,
              meta: {
                setup,
                analysis: {
                  bias: analysis?.bias ?? null,
                  trends: analysis?.trends ?? null,
                  entryCheck: setup?.reasons?.entryCheck ?? null,
                },
              },
            };
          }
        }
      }
    }

    // Equity curve point at candle close
    equityCurve.push({
      time: nowMs,
      equity,
      balance,
      open_position: open ? { side: open.side, entry: open.entryFill, sl: open.currentSl, tp: open.tp, qty: open.qty } : null,
      pending_entry: pending ? { side: pending.side, entry: pending.entryPrice, qty: pending.qty } : null,
    });
  }

  const summary = buildBacktestSummary({ trades, equityCurve, initialBalance });

  return { summary, trades, equity_curve: equityCurve };
}
