import { addIndicatorsToCandleRows } from './indicators.js';
import { INTERVALS, analyzeSymbolFromCandles } from './analyzeCore.js';
import { maybeMoveStopLoss } from './trailingCore.js';

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

function stripInternalPoint(pt) {
  if (!pt) return pt;
  // remove internal markers from output
  const { __pushed, __signal, ...rest } = pt;
  return rest;
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

export function buildBacktestSummary({ trades, initialBalance, finalEquity, maxDrawdown }) {
  const total = trades.length;
  const wins = trades.filter(t => t.netPnl > 0).length;
  const losses = trades.filter(t => t.netPnl < 0).length;
  const winRate = total ? wins / total : 0;
  const netProfit = trades.reduce((a, t) => a + t.netPnl, 0);
  const grossProfit = trades.filter(t => t.netPnl > 0).reduce((a, t) => a + t.netPnl, 0);
  const grossLoss = trades.filter(t => t.netPnl < 0).reduce((a, t) => a + Math.abs(t.netPnl), 0);
  const profitFactor = grossLoss > 0 ? grossProfit / grossLoss : null;

  return {
    initial_balance: initialBalance,
    final_equity: finalEquity ?? initialBalance,
    net_profit: netProfit,
    total_trades: total,
    wins,
    losses,
    win_rate: winRate,
    profit_factor: profitFactor,
    max_drawdown: maxDrawdown ?? 0,
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
  data,
  indicatorsFromDb = true,
  debug = false,
}) {
  if (!symbol) throw new Error('symbol required');

  if (!indicatorsFromDb) {
    for (const itv of Object.keys(data)) {
      addIndicatorsToCandleRows(data[itv]);
    }
  }

  const candles5 = data['5m'] ?? [];
  const start = Number(startTime);
  const end = Number(endTime);

  const ptr = {};
  for (const itv of INTERVALS) ptr[itv] = 0;

  let i0 = candles5.findIndex(c => c.open_time >= start);
  if (i0 < 0) i0 = candles5.length;

  let equity = Number(initialBalance);
  let balance = Number(initialBalance);

  // NOTE: we keep equity_curve output filtered (to reduce JSON size),
  // but still compute drawdown + final equity from the full run.
  const equityCurve = [];
  const trades = [];

  const equityContextPre = Math.max(0, Math.min(Number(process.env.BACKTEST_EQUITY_CONTEXT_PRE ?? 1), 5));
  const equityContextPost = Math.max(0, Math.min(Number(process.env.BACKTEST_EQUITY_CONTEXT_POST ?? 1), 5));
  const tradeContextBars = Math.max(0, Math.min(Number(process.env.BACKTEST_TRADE_CONTEXT_BARS ?? 1), 10));

  let peakEquity = equity;
  let maxDrawdown = 0;
  let finalEquity = equity;

  let lastEquityPoint = null;
  let postContextCountdown = 0;

  const debugStats = {
    bars_total: 0,
    analysis_null: 0,
    bias_wait: 0,
    bias_buy: 0,
    bias_sell: 0,
    entry_ok: 0,
    entry_fail: 0,
    setup_null: 0,
    setup_rr_lt_0: 0,
    pending_created: 0,
    pending_filled: 0,
    pending_never_filled: 0,
    margin_reject: 0,
    entry_reasons: {},
    htf_missing: { '1h': 0, '4h': 0, '1d': 0 },
  };

  let open = null;
  let pending = null;

  const stepMs = msForInterval('5m');

  for (let i = i0; i < candles5.length; i++) {
    const c5 = candles5[i];

    let didFillThisBar = false;
    let didCloseThisBar = false;
    let signalThisBar = false;
    if (c5.open_time > end) break;
    debugStats.bars_total += 1;

    const nowMs = c5.open_time + stepMs;

    // multi-TF sync using ONLY fully-closed candles
    const snapData = {};
    for (const itv of INTERVALS) {
      const arr = data[itv] ?? [];
      const tfMs = msForInterval(itv);
      let p = ptr[itv];
      while (p < arr.length && (arr[p].open_time + tfMs) <= nowMs) p++;
      ptr[itv] = p;
      snapData[itv] = arr.slice(0, p);
    }

    // Fill pending limit (option B)
    if (pending) {
      const filled = candleHitsPrice(c5, pending.entryPrice);
      if (filled) {
        didFillThisBar = true;
        debugStats.pending_filled += 1;
        const entryFill = applySlippage({ side: pending.side, price: pending.entryPrice, slippagePct });
        const notional = entryFill * pending.qty;
        const feeIn = calcFee({ notional, feeRate });
        balance -= feeIn;
        equity = balance;

        open = {
          ...pending,
          entryFill,
          entryTime: c5.open_time,
          entryIndex5m: i,
          initialSl: pending.sl,
          currentSl: pending.sl,
          tp: pending.tp,
          fees: feeIn,
          qtyOriginal: pending.qty,
        };
        pending = null;
      }
    }

    // Manage open position
    if (open) {
      const wantPTP = (process.env.BACKTEST_PTP_ENABLED ?? '1') === '1'
        && String(process.env.ANALYZE_STRATEGY ?? '').toUpperCase() === 'STACKED_TREND_STRATEGY';

      const hitSL = open.side === 'LONG'
        ? (c5.low <= open.currentSl)
        : (c5.high >= open.currentSl);

      if (hitSL) {
        const exitFill = applySlippage({ side: open.side, price: open.currentSl, slippagePct });
        const notionalOut = exitFill * open.qty;
        const feeOut = calcFee({ notional: notionalOut, feeRate });
        const gross = pnlLinearUSDT({ side: open.side, entry: open.entryFill, exit: exitFill, qty: open.qty });

        // entry fee already deducted at fill
        balance += (gross - feeOut);
        equity = balance;

        const entryIdx = Number.isInteger(open.entryIndex5m) ? open.entryIndex5m : null;
        const exitIdx = i;
        const ctxN = tradeContextBars;
        const ctxEntry = entryIdx == null ? null : candles5.slice(Math.max(0, entryIdx - ctxN), Math.min(candles5.length, entryIdx + ctxN + 1));
        const ctxExit = candles5.slice(Math.max(0, exitIdx - ctxN), Math.min(candles5.length, exitIdx + ctxN + 1));

        trades.push({
          symbol,
          side: open.side,
          entry_time: open.entryTime,
          exit_time: c5.open_time,
          entry: open.entryFill,
          exit: exitFill,
          qty: open.qtyOriginal || open.qty,
          sl_initial: open.initialSl,
          sl_final: open.currentSl,
          tp: open.tp,
          reason: open.ptp?.partialDone ? 'BE_AFTER_PARTIAL' : 'SL',
          grossPnl: (open.ptp?.accumulatedGross || 0) + gross,
          fees: open.fees + (open.ptp?.accumulatedFees || 0) + feeOut,
          netPnl: ((open.ptp?.accumulatedGross || 0) + gross) - (open.fees + (open.ptp?.accumulatedFees || 0) + feeOut),
          leverage,
          margin_used: (open.entryFill * (open.qtyOriginal || open.qty)) / Math.max(1, Number(leverage) || 1),
          meta: open.meta ?? null,
          context_candles: {
            entry_5m: ctxEntry,
            exit_5m: ctxExit,
          },
        });

        didCloseThisBar = true;
        open = null;
      } else {
        if (wantPTP) {
          if (open.ptp == null) {
            const R = Math.abs(open.entryFill - open.initialSl);
            open.ptp = {
              R,
              partialDone: false,
              partialPrice: open.side === 'LONG' ? (open.entryFill + R) : (open.entryFill - R),
              finalPrice: open.side === 'LONG' ? (open.entryFill + 2 * R) : (open.entryFill - 2 * R),
              accumulatedGross: 0,
              accumulatedFees: 0,
            };
          }

          // Partial +1R
          if (!open.ptp.partialDone) {
            const hitPartial = open.side === 'LONG'
              ? (c5.high >= open.ptp.partialPrice)
              : (c5.low <= open.ptp.partialPrice);

            if (hitPartial) {
              const closeQty = open.qty * 0.5;
              const exitFill = applySlippage({ side: open.side, price: open.ptp.partialPrice, slippagePct });
              const notionalOut = exitFill * closeQty;
              const feeOut = calcFee({ notional: notionalOut, feeRate });
              const gross = pnlLinearUSDT({ side: open.side, entry: open.entryFill, exit: exitFill, qty: closeQty });

              balance += (gross - feeOut);
              open.ptp.accumulatedGross += gross;
              open.ptp.accumulatedFees += feeOut;

              open.qty -= closeQty;
              open.ptp.partialDone = true;

              // Breakeven + fee buffer
              const feeBuf = open.entryFill * clamp(feeRate, 0, 0.01) * 2;
              open.currentSl = open.side === 'LONG' ? (open.entryFill + feeBuf) : (open.entryFill - feeBuf);

              // same-candle BE check (conservative)
              const hitBeSame = open.side === 'LONG'
                ? (c5.low <= open.currentSl)
                : (c5.high >= open.currentSl);

              if (hitBeSame) {
                const exitFill2 = applySlippage({ side: open.side, price: open.currentSl, slippagePct });
                const notionalOut2 = exitFill2 * open.qty;
                const feeOut2 = calcFee({ notional: notionalOut2, feeRate });
                const gross2 = pnlLinearUSDT({ side: open.side, entry: open.entryFill, exit: exitFill2, qty: open.qty });

                balance += (gross2 - feeOut2);

                const entryIdx = Number.isInteger(open.entryIndex5m) ? open.entryIndex5m : null;
                const exitIdx = i;
                const ctxN = tradeContextBars;
                const ctxEntry = entryIdx == null ? null : candles5.slice(Math.max(0, entryIdx - ctxN), Math.min(candles5.length, entryIdx + ctxN + 1));
                const ctxExit = candles5.slice(Math.max(0, exitIdx - ctxN), Math.min(candles5.length, exitIdx + ctxN + 1));

                trades.push({
                  symbol,
                  side: open.side,
                  entry_time: open.entryTime,
                  exit_time: c5.open_time,
                  entry: open.entryFill,
                  exit: exitFill2,
                  qty: open.qtyOriginal,
                  sl_initial: open.initialSl,
                  sl_final: open.currentSl,
                  tp: open.ptp.finalPrice,
                  reason: 'PTP_1R_THEN_BE',
                  grossPnl: open.ptp.accumulatedGross + gross2,
                  fees: open.fees + open.ptp.accumulatedFees + feeOut2,
                  netPnl: (open.ptp.accumulatedGross + gross2) - (open.fees + open.ptp.accumulatedFees + feeOut2),
                  leverage,
                  margin_used: (open.entryFill * open.qtyOriginal) / Math.max(1, Number(leverage) || 1),
                  meta: { ...open.meta, ptp: open.ptp },
                  context_candles: {
                    entry_5m: ctxEntry,
                    exit_5m: ctxExit,
                  },
                });

                didCloseThisBar = true;
                open = null;
              }
            }
          }

          // Final +2R
          if (open && open.ptp?.partialDone) {
            const hitFinal = open.side === 'LONG'
              ? (c5.high >= open.ptp.finalPrice)
              : (c5.low <= open.ptp.finalPrice);

            if (hitFinal) {
              const exitFill = applySlippage({ side: open.side, price: open.ptp.finalPrice, slippagePct });
              const notionalOut = exitFill * open.qty;
              const feeOut = calcFee({ notional: notionalOut, feeRate });
              const gross = pnlLinearUSDT({ side: open.side, entry: open.entryFill, exit: exitFill, qty: open.qty });

              balance += (gross - feeOut);
              equity = balance;

              const entryIdx = Number.isInteger(open.entryIndex5m) ? open.entryIndex5m : null;
              const exitIdx = i;
              const ctxN = tradeContextBars;
              const ctxEntry = entryIdx == null ? null : candles5.slice(Math.max(0, entryIdx - ctxN), Math.min(candles5.length, entryIdx + ctxN + 1));
              const ctxExit = candles5.slice(Math.max(0, exitIdx - ctxN), Math.min(candles5.length, exitIdx + ctxN + 1));

              trades.push({
                symbol,
                side: open.side,
                entry_time: open.entryTime,
                exit_time: c5.open_time,
                entry: open.entryFill,
                exit: exitFill,
                qty: open.qtyOriginal,
                sl_initial: open.initialSl,
                sl_final: open.currentSl,
                tp: open.ptp.finalPrice,
                reason: 'TP_2R_AFTER_PTP',
                grossPnl: open.ptp.accumulatedGross + gross,
                fees: open.fees + open.ptp.accumulatedFees + feeOut,
                netPnl: (open.ptp.accumulatedGross + gross) - (open.fees + open.ptp.accumulatedFees + feeOut),
                leverage,
                margin_used: (open.entryFill * open.qtyOriginal) / Math.max(1, Number(leverage) || 1),
                meta: { ...open.meta, ptp: open.ptp },
                context_candles: {
                  entry_5m: ctxEntry,
                  exit_5m: ctxExit,
                },
              });

              didCloseThisBar = true;
              open = null;
            }
          }
        } else {
          // Legacy trailing/TP
          const mv = maybeMoveStopLoss({
            side: open.side,
            entry: open.entryFill,
            initialSl: open.initialSl,
            currentSl: open.currentSl,
            price: c5.close,
          });
          if (mv.newSl != null) {
            if (open.side === 'LONG') open.currentSl = Math.max(open.currentSl, mv.newSl);
            else open.currentSl = Math.min(open.currentSl, mv.newSl);
          }

          const hitTP = open.side === 'LONG' ? (c5.high >= open.tp) : (c5.low <= open.tp);
          if (hitTP) {
            const exitFill = applySlippage({ side: open.side, price: open.tp, slippagePct });
            const notionalOut = exitFill * open.qty;
            const feeOut = calcFee({ notional: notionalOut, feeRate });
            const gross = pnlLinearUSDT({ side: open.side, entry: open.entryFill, exit: exitFill, qty: open.qty });

            balance += (gross - feeOut);
            equity = balance;

            const entryIdx = Number.isInteger(open.entryIndex5m) ? open.entryIndex5m : null;
            const exitIdx = i;
            const ctxN = tradeContextBars;
            const ctxEntry = entryIdx == null ? null : candles5.slice(Math.max(0, entryIdx - ctxN), Math.min(candles5.length, entryIdx + ctxN + 1));
            const ctxExit = candles5.slice(Math.max(0, exitIdx - ctxN), Math.min(candles5.length, exitIdx + ctxN + 1));

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
              reason: 'TP',
              grossPnl: gross,
              fees: open.fees + feeOut,
              netPnl: gross - open.fees - feeOut,
              leverage,
              margin_used: (open.entryFill * open.qty) / Math.max(1, Number(leverage) || 1),
              meta: open.meta ?? null,
              context_candles: {
                entry_5m: ctxEntry,
                exit_5m: ctxExit,
              },
            });

            didCloseThisBar = true;
            open = null;
          }
        }
      }
    }

    // Analyze + open new pending
    if (!open && !pending) {
      const analysis = analyzeSymbolFromCandles({ symbol, data: snapData, nowMs });
      if (!analysis) {
        debugStats.analysis_null += 1;
      } else {
        if (analysis.bias === 'WAIT') debugStats.bias_wait += 1;
        if (analysis.bias === 'BUY') debugStats.bias_buy += 1;
        if (analysis.bias === 'SELL') debugStats.bias_sell += 1;
        if (!analysis.setup) debugStats.setup_null += 1;

        const ec = analysis.entryCheck;
        if (ec?.ok) debugStats.entry_ok += 1;
        else debugStats.entry_fail += 1;

        const reason = ec?.reason;
        if (reason) debugStats.entry_reasons[reason] = (debugStats.entry_reasons[reason] ?? 0) + 1;
      }

      const setup = analysis?.setup;
      if (setup && (setup.action === 'BUY' || setup.action === 'SELL')) {
        signalThisBar = true;
        const side = sideFromAction(setup.action);
        const entry = Number(setup.entry);
        const sl = Number(setup.sl);
        const tp = Number(setup.tp);
        const stopDist = Math.abs(entry - sl);
        if (stopDist > 0 && Number.isFinite(stopDist)) {
          const risk$ = balance * clamp(riskPerTrade, 0, 1);
          const qty = risk$ / stopDist;

          const notional = entry * qty;
          const marginNeed = notional / Math.max(1, Number(leverage) || 1);
          if (marginNeed <= balance && qty > 0) {
            debugStats.pending_created += 1;
            pending = {
              side,
              entryPrice: entry,
              sl,
              tp,
              qty,
              placedTime: c5.open_time,
              meta: { setup, analysis: { bias: analysis?.bias ?? null, entryCheck: analysis?.entryCheck ?? null } },
            };
          } else {
            debugStats.margin_reject += 1;
          }
        } else {
          debugStats.setup_rr_lt_0 += 1;
        }
      }
    }

    const equityMtM = balance + (open ? pnlLinearUSDT({ side: open.side, entry: open.entryFill, exit: c5.close, qty: open.qty }) : 0);
    equity = equityMtM;

    // Update drawdown stats from the FULL run (not filtered)
    finalEquity = equityMtM;
    if (equityMtM > peakEquity) peakEquity = equityMtM;
    const dd = peakEquity > 0 ? (peakEquity - equityMtM) / peakEquity : 0;
    if (dd > maxDrawdown) maxDrawdown = dd;

    // Filter equity curve output: only keep bars with trading activity / signals,
    // plus optional context bars around those events.
    const point = {
      time: nowMs,
      equity: equityMtM,
      balance,
      unrealized_pnl: equityMtM - balance,
      open_position: open ? { side: open.side, entry: open.entryFill, sl: open.currentSl, tp: open.tp, qty: open.qty } : null,
      pending_entry: pending ? { side: pending.side, entry: pending.entryPrice, qty: pending.qty } : null,
      close: c5.close,
    };

    // Decide if this bar is "interesting"
    // - open position exists
    // - pending exists
    // - fill/close happened on this candle
    // - analysis produced a valid setup (signal)
    // - within post-context window
    let interesting = Boolean(open || pending || didFillThisBar || didCloseThisBar || signalThisBar);

    if (!interesting && postContextCountdown > 0) interesting = true;

    if (interesting) {
      // include N bars before the first interesting point
      if (equityContextPre > 0 && lastEquityPoint && lastEquityPoint.__pushed !== true) {
        equityCurve.push(stripInternalPoint(lastEquityPoint));
        lastEquityPoint.__pushed = true;
      }

      equityCurve.push(stripInternalPoint(point));
      postContextCountdown = Math.max(postContextCountdown, equityContextPost);
      if (postContextCountdown > 0) postContextCountdown -= 1;
      point.__pushed = true;
    } else {
      // not interesting: keep last point as potential pre-context
      postContextCountdown = 0;
    }

    lastEquityPoint = { ...point, __pushed: point.__pushed ?? false, __signal: signalThisBar };
  }

  if (pending) debugStats.pending_never_filled += 1;

  const summary = buildBacktestSummary({ trades, initialBalance, finalEquity, maxDrawdown });
  return { summary, trades, equity_curve: equityCurve, debug: debug ? debugStats : undefined };
}
