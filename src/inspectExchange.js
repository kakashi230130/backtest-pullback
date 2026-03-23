import { logger } from './logger.js';
import { getFuturesPositionRisk, getFuturesOpenOrders } from './binanceTradeClient.js';

async function main() {
  const symbol = process.env.SYMBOL ?? 'ETHUSDT';
  const positions = await getFuturesPositionRisk();
  const p = positions.find(x => x.symbol === symbol);
  console.log({ symbol, positionAmt: p?.positionAmt, entryPrice: p?.entryPrice, unrealizedProfit: p?.unRealizedProfit });

  const openOrders = await getFuturesOpenOrders({ symbol });
  console.log({ openOrdersCount: openOrders.length, openOrders });
}

main().catch((e) => {
  logger.error({ err: e }, 'inspectExchange failed');
  process.exit(1);
});
