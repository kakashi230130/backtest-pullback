import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';

import { INTERVALS } from '../strategy/analyzeCore.js';
import { loadCandlesMultiTf } from './dataLoader.js';
import { runBacktest } from './engine.js';

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    const key = a.slice(2);
    const val = argv[i + 1];
    if (val == null || val.startsWith('--')) {
      out[key] = true;
    } else {
      out[key] = val;
      i++;
    }
  }
  return out;
}

function parseTimeMs(x) {
  if (x == null) return null;
  if (/^\d+$/.test(String(x))) return Number(x);
  const t = Date.parse(String(x));
  if (!Number.isFinite(t)) throw new Error(`Invalid time: ${x}`);
  return t;
}

async function main() {
  const args = parseArgs(process.argv);

  const symbol = args.symbol ?? process.env.BACKTEST_SYMBOL;
  const startTime = parseTimeMs(args.start ?? args.startTime ?? process.env.BACKTEST_START_TIME);
  const endTime = parseTimeMs(args.end ?? args.endTime ?? process.env.BACKTEST_END_TIME);

  if (!symbol) throw new Error('Missing --symbol');
  if (!startTime) throw new Error('Missing --start (ms or ISO)');
  if (!endTime) throw new Error('Missing --end (ms or ISO)');

  const initialBalance = Number(args.initial_balance ?? args.initialBalance ?? process.env.BACKTEST_INITIAL_BALANCE ?? 1000);
  const riskPerTrade = Number(args.risk_per_trade ?? args.riskPerTrade ?? process.env.BACKTEST_RISK_PER_TRADE ?? 0.01);
  const leverage = Number(args.leverage ?? process.env.BACKTEST_LEVERAGE ?? 10);
  const feeRate = Number(args.fee ?? process.env.BACKTEST_FEE ?? 0.0004);
  const slippagePct = Number(args.slippage ?? process.env.BACKTEST_SLIPPAGE ?? 0);

  // warmup: by default load 300 bars of 1d at most => 300d, but that's too much.
  // We'll just load 7 days extra for 5m and let other TFs naturally have enough bars.
  const warmupMs = Number(args.warmup_ms ?? process.env.BACKTEST_WARMUP_MS ?? (7 * 24 * 60 * 60 * 1000));

  const { data, indicatorsFromDb } = await loadCandlesMultiTf({
    symbol,
    intervals: INTERVALS,
    startTime,
    endTime,
    warmupMs,
  });

  const debug = String(args.debug ?? process.env.BACKTEST_DEBUG ?? '0') === '1';

  const result = runBacktest({
    symbol,
    startTime,
    endTime,
    initialBalance,
    riskPerTrade,
    leverage,
    feeRate,
    slippagePct,
    data,
    indicatorsFromDb,
    debug,
  });

  const outPath = args.out ?? null;
  if (outPath) {
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, JSON.stringify(result, null, 2));
  }

  console.log(JSON.stringify({
    ok: true,
    indicatorsFromDb,
    summary: result.summary,
    trades: result.trades.length,
    equity_points: result.equity_curve.length,
    debug: debug ? result.debug : undefined,
    out: outPath ?? null,
  }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
