import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';

import { INTERVALS } from './analyzeCore.js';
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

  // Support both flag-style args:
  //   --symbol BTCUSDT --start ... --end ... --out output/result.json
  // and positional args (some npm setups strip leading --flags when forwarding):
  //   BTCUSDT 2025-01-01T00:00:00Z 2026-01-01T00:00:00Z output/result.json
  const positional = process.argv.slice(2).filter(a => !String(a).startsWith('--'));

  const symbol = args.symbol ?? positional[0] ?? process.env.BACKTEST_SYMBOL;
  const startTime = parseTimeMs(args.start ?? args.startTime ?? positional[1] ?? process.env.BACKTEST_START_TIME);
  const endTime = parseTimeMs(args.end ?? args.endTime ?? positional[2] ?? process.env.BACKTEST_END_TIME);

  if (!symbol) throw new Error('Missing --symbol');
  if (!startTime) throw new Error('Missing --start (ms or ISO)');
  if (!endTime) throw new Error('Missing --end (ms or ISO)');

  const initialBalance = Number(args.initial_balance ?? args.initialBalance ?? process.env.BACKTEST_INITIAL_BALANCE ?? 1000);
  const riskPerTrade = Number(args.risk_per_trade ?? args.riskPerTrade ?? process.env.BACKTEST_RISK_PER_TRADE ?? 0.01);
  const leverage = Number(args.leverage ?? process.env.BACKTEST_LEVERAGE ?? 10);
  const feeRate = Number(args.fee ?? process.env.BACKTEST_FEE ?? 0.0004);
  const slippagePct = Number(args.slippage ?? process.env.BACKTEST_SLIPPAGE ?? 0);

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

  const outPath = args.out ?? positional[3] ?? null;
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
