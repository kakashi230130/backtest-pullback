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

// CẬP NHẬT: Thêm yếu tố ngẫu nhiên để trượt giá chân thực hơn
function applySlippage({ side, price, slippagePct }) {
  const p = Number(price);
  const randomFactor = 0.5 + Math.random(); // Random từ 0.5x đến 1.5x slippage config
  const s = clamp(slippagePct * randomFactor, 0, 0.01); 
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
  slippagePct = 0.0002, // Đặt mặc định slippage 0.02%
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

  const mainTf = '5m';
  const candles5 = data[mainTf] ?? [];
  const start = Number(startTime);
  const end = Number(endTime);

  const ptr = {};
  for (const itv of INTERVALS) ptr[itv] = 0;

  let i0 = candles5.findIndex(c => c.open_time >= start);
  if (i0 < 0) i0 = candles5.length;

  let equity = Number(initialBalance);
  let balance = Number(initialBalance);

  const equityCurve = [];
  const trades = [];

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
    if (c5.open_time > end) break;
    debugStats.bars_total += 1;

    const nowMs = c5.open_time + stepMs; 

    const snapData = {};
    for (const itv of INTERVALS) {
      const arr = data[itv] ?? [];
      const tfMs = msForInterval(itv);
      let p = ptr[itv];
      while (p < arr.length && (arr[p].open_time + tfMs) <= nowMs) p++;
      ptr[itv] = p;
      snapData[itv] = arr.slice(0, p);
    }

    if (pending) {
      const filled = candleHitsPrice(c5, pending.entryPrice);
      if (filled) {
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
          initialSl: pending.sl,
          currentSl: pending.sl,
          tp: pending.tp,
          fees: feeIn,
          maxFavorable: entryFill,
          minFavorable: entryFill,
          qtyOriginal: pending.qty, 
        };
        pending = null;
      }
    }

    let unrealized = 0;
    if (open) {
      const wantPTP = (process.env.BACKTEST_PTP_ENABLED ?? '1') === '1' && String(process.env.ANALYZE_STRATEGY ?? '').toUpperCase() === 'STACKED_TREND_STRATEGY';

      unrealized = pnlLinearUSDT({ side: open.side, entry: open.entryFill, exit: c5.close, qty: open.qty });

      const hitSL = open.side === 'LONG'
        ? (c5.low <= open.currentSl)
        : (c5.high >= open.currentSl);

      if (hitSL) {
        const exitFill = applySlippage({ side: open.side, price: open.currentSl, slippagePct });
        const notionalOut = exitFill * open.qty;
        const feeOut = calcFee({ notional: notionalOut, feeRate });
        const gross = pnlLinearUSDT({ side: open.side, entry: open.entryFill, exit: exitFill, qty: open.qty });
        
        balance += (gross - feeOut);
        equity = balance;

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
          reason: open.ptp?.partialDone ? (open.currentSl !== open.initialSl && open.currentSl !== open.entryFill ? 'TRAILING_STOP' : 'BE_AFTER_PARTIAL') : 'SL',
          grossPnl: (open.ptp?.accumulatedGross || 0) + gross,
          fees: open.fees + (open.ptp?.accumulatedFees || 0) + feeOut,
          netPnl: ((open.ptp?.accumulatedGross || 0) + gross) - (open.fees + (open.ptp?.accumulatedFees || 0) + feeOut),
          leverage,
          margin_used: (open.entryFill * (open.qtyOriginal || open.qty)) / Math.max(1, Number(leverage) || 1),
          meta: open.meta ?? null,
        });

        open = null;
        unrealized = 0;
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

              const feeBuf = open.entryFill * clamp(feeRate, 0, 0.01) * 2;
              const be = open.side === 'LONG' ? (open.entryFill + feeBuf) : (open.entryFill - feeBuf);
              open.currentSl = be;
            }
          }

          if (open && open.ptp?.partialDone) {
            // CẬP NHẬT: Trailing Stop cho 50% còn lại. Nếu giá chạy được > 1.5R thì dời SL lên khóa lãi +0.5R
            const favorable = open.side === 'LONG' ? (c5.high - open.entryFill) : (open.entryFill - c5.low);
            if (favorable >= 1.5 * open.ptp.R) {
              const trailPrice = open.side === 'LONG' ? (open.entryFill + 0.5 * open.ptp.R) : (open.entryFill - 0.5 * open.ptp.R);
              if (open.side === 'LONG') open.currentSl = Math.max(open.currentSl, trailPrice);
              else open.currentSl = Math.min(open.currentSl, trailPrice);
            }

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
              });

              open = null;
              unrealized = 0;
            }
          }
        } else {
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

          const hitTP = open.side === 'LONG'
            ? (c5.high >= open.tp)
            : (c5.low <= open.tp);

          if (hitTP) {
            const exitFill = applySlippage({ side: open.side, price: open.tp, slippagePct });
            const notionalOut = exitFill * open.qty;
            const feeOut = calcFee({ notional: notionalOut, feeRate });
            const gross = pnlLinearUSDT({ side: open.side, entry: open.entryFill, exit: exitFill, qty: open.qty });
            
            balance += (gross - feeOut);
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
              reason: 'TP',
              grossPnl: gross,
              fees: open.fees + feeOut,
              netPnl: gross - open.fees - feeOut,
              leverage,
              margin_used: (open.entryFill * open.qty) / Math.max(1, Number(leverage) || 1),
              meta: open.meta ?? null,
            });

            open = null;
            unrealized = 0;
          }
        }
      }
    }

    if (!open && !pending) {
      const analysis = analyzeSymbolFromCandles({ symbol, data: snapData, nowMs });
      if (!analysis) {
        debugStats.analysis_null += 1;
      } else {
        if (analysis.bias === 'WAIT') debugStats.bias_wait += 1;
        if (analysis.bias === 'BUY') debugStats.bias_buy += 1;
        if (analysis.bias === 'SELL') debugStats.bias_sell += 1;
        if (!analysis.setup) debugStats.setup_null += 1;

        const s1h = analysis.snapshots?.c1h;
        const s4h = analysis.snapshots?.c4h;
        const s1d = analysis.snapshots?.c1d;
        if (s1h?.ma20 == null || s1h?.ma50 == null || s1h?.rsi == null) debugStats.htf_missing['1h'] += 1;
        if (s4h?.ma20 == null || s4h?.ma50 == null || s4h?.rsi == null) debugStats.htf_missing['4h'] += 1;
        if (s1d?.ma20 == null || s1d?.ma50 == null || s1d?.rsi == null) debugStats.htf_missing['1d'] += 1;

        const ec = analysis.entryCheck;
        if (ec?.ok) debugStats.entry_ok += 1;
        else debugStats.entry_fail += 1;

        const reason = ec?.reason;
        if (reason) {
          debugStats.entry_reasons[reason] = (debugStats.entry_reasons[reason] ?? 0) + 1;
        }
      }

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
              meta: {
                setup,
                analysis: {
                  bias: analysis?.bias ?? null,
                  trends: analysis?.trends ?? null,
                  entryCheck: setup?.reasons?.entryCheck ?? null,
                },
              },
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

    equityCurve.push({
      time: nowMs,
      equity: equityMtM,
      balance,
      unrealized_pnl: equityMtM - balance,
      open_position: open ? { side: open.side, entry: open.entryFill, sl: open.currentSl, tp: open.tp, qty: open.qty } : null,
      pending_entry: pending ? { side: pending.side, entry: pending.entryPrice, qty: pending.qty } : null,
      close: c5.close,
    });
  }

  if (pending) debugStats.pending_never_filled += 1;

  const summary = buildBacktestSummary({ trades, equityCurve, initialBalance });

  return { summary, trades, equity_curve: equityCurve, debug: debug ? debugStats : undefined };
}
