import { logger } from './logger.js';
import { tradingEnabled } from './config.js';
import { getFuturesPositionRisk, placeFuturesMarketOrder } from './binanceTradeClient.js';

async function main() {
  const symbol = process.env.SYMBOL ?? 'ETHUSDT';
  const positions = await getFuturesPositionRisk();
  const p = positions.find(x => x.symbol === symbol);
  const amt = Number(p?.positionAmt ?? 0);
  if (!Number.isFinite(amt) || amt === 0) {
    console.log({ symbol, closed: false, reason: 'NO_POSITION' });
    return;
  }

  const side = amt > 0 ? 'SELL' : 'BUY';
  const positionSide = amt > 0 ? 'LONG' : 'SHORT';
  const qty = Math.abs(amt);

  if (!tradingEnabled) {
    console.log({ symbol, closed: false, reason: 'TRADING_ENABLED!=1 (dry-run)' });
    return;
  }

  const res = await placeFuturesMarketOrder({
    symbol,
    side,
    positionSide,
    quantity: qty,
    reduceOnly: null,
  });

  console.log({ symbol, closed: true, side, positionSide, qty, res });
}

main().catch((e) => {
  logger.error({ err: e }, 'closePosition failed');
  process.exit(1);
});
