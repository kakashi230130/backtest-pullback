import 'dotenv/config';

// Safety switch: require explicit opt-in before sending real orders
export const tradingEnabled = process.env.TRADING_ENABLED === '1';
export const exchangeName = process.env.EXCHANGE_NAME ?? 'binance_usdtm_demo';

// If your account requires Algo Order endpoints for SL/TP (-4120), enable this.
export const algoSltpEnabled = process.env.ALGO_SLTP_ENABLED === '1';
// Binance migrated conditional orders to Algo Service (see derivatives changelog 2025-11-06).
// Default endpoint for conditional algo orders:
// POST /fapi/v1/algoOrder
export const algoOrderPath = process.env.BINANCE_ALGO_ORDER_PATH ?? '/fapi/v1/algoOrder';


function required(name, fallback) {
  const v = process.env[name] ?? fallback;
  if (v === undefined || v === null || v === '') {
    throw new Error(`Missing env var: ${name}`);
  }
  return v;
}

export const config = {
  db: {
    host: required('DB_HOST', '127.0.0.1'),
    port: Number(process.env.DB_PORT ?? 3306),
    user: required('DB_USER', 'root'),
    password: process.env.DB_PASSWORD ?? '',
    database: required('DB_NAME', 'binance_candles'),
  },
  binance: {
    apiKey: process.env.BINANCE_API_KEY ?? '',
    apiSecret: process.env.BINANCE_API_SECRET ?? '',

    // Public (market data) endpoints.
    // We use USDT-M Futures klines by default (fapi) so OHLC matches futures charts.
    baseUrl: process.env.BINANCE_PUBLIC_BASE_URL ?? 'https://fapi.binance.com',

    // Trading endpoints.
    // If you're using Binance Futures *testnet*, set BINANCE_TRADE_BASE_URL=https://testnet.binancefuture.com
    tradeBaseUrl: process.env.BINANCE_TRADE_BASE_URL ?? process.env.BINANCE_PUBLIC_BASE_URL ?? 'https://api.binance.com',
  },
  symbols: (process.env.SYMBOLS ?? 'BTCUSDT')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean),
  logLevel: process.env.LOG_LEVEL ?? 'info',
};
