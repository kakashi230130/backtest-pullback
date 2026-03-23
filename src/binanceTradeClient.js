import axios from 'axios';
import crypto from 'crypto';
import { config } from './config.js';

function signQuery(queryString, secret) {
  return crypto.createHmac('sha256', secret).update(queryString).digest('hex');
}

function toQuery(params) {
  const sp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === null || v === '') continue;
    sp.set(k, String(v));
  }
  return sp.toString();
}

export const binanceTradeHttp = axios.create({
  baseURL: config.binance.tradeBaseUrl,
  timeout: 30_000,
  headers: config.binance.apiKey ? { 'X-MBX-APIKEY': config.binance.apiKey } : {},
});

// --- Helpers to match Binance symbol precision (tickSize/stepSize) ---
let exchangeInfoCache = null;
const symbolFilterCache = new Map();

function stepRoundDown(value, step) {
  const v = Number(value);
  const s = Number(step);
  if (!Number.isFinite(v) || !Number.isFinite(s) || s <= 0) return value;
  const n = Math.floor(v / s) * s;
  const decimals = Math.max(0, (String(step).split('.')[1] ?? '').length);
  return Number(n.toFixed(decimals));
}

async function getFuturesExchangeInfo() {
  if (exchangeInfoCache) return exchangeInfoCache;
  const { data } = await binanceTradeHttp.get('/fapi/v1/exchangeInfo');
  exchangeInfoCache = data;
  return data;
}

async function getSymbolFilters(symbol) {
  if (symbolFilterCache.has(symbol)) return symbolFilterCache.get(symbol);
  const info = await getFuturesExchangeInfo();
  const s = info.symbols?.find(x => x.symbol === symbol);
  if (!s) throw new Error(`Symbol not found in exchangeInfo: ${symbol}`);

  const pf = s.filters?.find(f => f.filterType === 'PRICE_FILTER');
  const lf = s.filters?.find(f => f.filterType === 'LOT_SIZE');

  const out = {
    tickSize: pf?.tickSize ? Number(pf.tickSize) : null,
    stepSize: lf?.stepSize ? Number(lf.stepSize) : null,
  };
  symbolFilterCache.set(symbol, out);
  return out;
}

async function normalizeOrder({ symbol, price, quantity, stopPrice }) {
  const f = await getSymbolFilters(symbol);
  const out = { price, quantity, stopPrice };
  if (out.price != null && f.tickSize) out.price = stepRoundDown(out.price, f.tickSize);
  if (out.stopPrice != null && f.tickSize) out.stopPrice = stepRoundDown(out.stopPrice, f.tickSize);
  if (out.quantity != null && f.stepSize) out.quantity = stepRoundDown(out.quantity, f.stepSize);
  return out;
}

let dualSideCache = null;
let serverTimeOffsetMs = 0;
let lastTimeSyncAt = 0;

async function syncServerTimeIfNeeded() {
  const now = Date.now();
  if (now - lastTimeSyncAt < 60_000) return; // sync at most once per minute
  const { data } = await binanceTradeHttp.get('/fapi/v1/time');
  const serverTime = Number(data?.serverTime);
  if (Number.isFinite(serverTime)) {
    serverTimeOffsetMs = serverTime - now;
    lastTimeSyncAt = now;
  }
}

function nowWithServerOffset() {
  // small safety margin to avoid "timestamp ahead" errors on jittery clocks
  return Date.now() + serverTimeOffsetMs - 1500;
}

async function signedRequest(method, path, params) {
  if (!config.binance.apiKey || !config.binance.apiSecret) {
    throw new Error('Missing BINANCE_API_KEY / BINANCE_API_SECRET for trading endpoints');
  }

  await syncServerTimeIfNeeded();

  const base = {
    recvWindow: 20_000,
    timestamp: nowWithServerOffset(),
    ...params,
  };

  const qs = toQuery(base);
  const signature = signQuery(qs, config.binance.apiSecret);
  const url = `${path}?${qs}&signature=${signature}`;

  try {
    const { data } = await binanceTradeHttp.request({ method, url });
    return data;
  } catch (err) {
    const code = err?.response?.data?.code;
    if (code === -1021) {
      // Timestamp out of recvWindow: resync server time and retry once.
      lastTimeSyncAt = 0;
      await syncServerTimeIfNeeded();

      const base2 = {
        ...base,
        recvWindow: 20_000,
        timestamp: nowWithServerOffset(),
      };
      const qs2 = toQuery(base2);
      const sig2 = signQuery(qs2, config.binance.apiSecret);
      const url2 = `${path}?${qs2}&signature=${sig2}`;

      const { data } = await binanceTradeHttp.request({ method, url: url2 });
      return data;
    }
    throw err;
  }
}

// NOTE: This implementation targets USDT-M Futures endpoints (fapi).
// For Spot, endpoints differ (/api/v3/order). Adjust if you trade spot.

async function isDualSidePosition() {
  if (dualSideCache != null) return dualSideCache;
  const data = await signedRequest('GET', '/fapi/v1/positionSide/dual', {});
  // { dualSidePosition: true/false }
  dualSideCache = Boolean(data?.dualSidePosition);
  return dualSideCache;
}

async function withPositionSide(params, entrySide /* BUY|SELL */, positionSideOverride /* LONG|SHORT|null */) {
  // If account is in Hedge Mode (dualSidePosition=true), positionSide is required and must be LONG/SHORT.
  // In One-way Mode, positionSide MUST NOT be sent.
  const dual = await isDualSidePosition();
  if (!dual) {
    // Defensive: strip positionSide if any caller passed it.
    // (Some call-sites were written for hedge mode and reuse LONG/SHORT in DB.)
    const { positionSide, ...rest } = params ?? {};
    return rest;
  }

  const positionSide = positionSideOverride ?? (entrySide === 'BUY' ? 'LONG' : 'SHORT');
  return { ...params, positionSide };
}

export async function setFuturesLeverage({ symbol, leverage }) {
  return signedRequest('POST', '/fapi/v1/leverage', { symbol, leverage });
}

export async function placeFuturesLimitOrder({ symbol, side, quantity, price, timeInForce = 'GTC', newClientOrderId }) {
  const n = await normalizeOrder({ symbol, price, quantity });
  const params = await withPositionSide(
    {
      symbol,
      side,
      type: 'LIMIT',
      timeInForce,
      quantity: n.quantity,
      price: n.price,
      newClientOrderId,
      newOrderRespType: 'RESULT',
    },
    side,
    null,
  );
  return signedRequest('POST', '/fapi/v1/order', params);
}

export async function placeFuturesStopLossMarket({
  symbol,
  side,
  stopPrice,
  quantity,
  closePosition = false,
  reduceOnly = true,
  positionSide,
  newClientOrderId,
}) {
  // side here is the *closing* side (opposite of entry): LONG closes with SELL, SHORT closes with BUY
  const n = await normalizeOrder({ symbol, stopPrice, quantity: closePosition ? null : quantity });

  const base = {
    symbol,
    side,
    type: 'STOP_MARKET',
    stopPrice: n.stopPrice,
    workingType: 'MARK_PRICE',
    newClientOrderId,
    newOrderRespType: 'RESULT',
  };

  // Binance allows either quantity OR closePosition on some accounts.
  if (closePosition) base.closePosition = 'true';
  else base.quantity = n.quantity;

  if (reduceOnly != null) base.reduceOnly = String(reduceOnly);

  const params = await withPositionSide(
    base,
    // used only to derive positionSide if override is null
    side === 'BUY' ? 'SELL' : 'BUY',
    positionSide ?? null,
  );

  return signedRequest('POST', '/fapi/v1/order', params);
}

export async function placeFuturesTakeProfitMarket({
  symbol,
  side,
  stopPrice,
  quantity,
  closePosition = false,
  reduceOnly = null,
  positionSide,
  newClientOrderId,
}) {
  const n = await normalizeOrder({ symbol, stopPrice, quantity: closePosition ? null : quantity });

  const base = {
    symbol,
    side,
    type: 'TAKE_PROFIT_MARKET',
    stopPrice: n.stopPrice,
    workingType: 'MARK_PRICE',
    newClientOrderId,
    newOrderRespType: 'RESULT',
  };

  if (closePosition) base.closePosition = 'true';
  else base.quantity = n.quantity;

  if (reduceOnly != null) base.reduceOnly = String(reduceOnly);

  const params = await withPositionSide(
    base,
    side === 'BUY' ? 'SELL' : 'BUY',
    positionSide ?? null,
  );

  return signedRequest('POST', '/fapi/v1/order', params);
}

// --- Limit-style conditional orders (fallback when *_MARKET returns -4120) ---
export async function placeFuturesStopLossLimit({
  symbol,
  side,
  stopPrice,
  price,
  quantity,
  timeInForce = 'GTC',
  reduceOnly = null,
  positionSide,
  newClientOrderId,
}) {
  const n = await normalizeOrder({ symbol, stopPrice, price, quantity });

  const base = {
    symbol,
    side,
    type: 'STOP',
    timeInForce,
    stopPrice: n.stopPrice,
    price: n.price,
    quantity: n.quantity,
    workingType: 'MARK_PRICE',
    newClientOrderId,
    newOrderRespType: 'RESULT',
  };
  if (reduceOnly != null) base.reduceOnly = String(reduceOnly);

  const params = await withPositionSide(
    base,
    side === 'BUY' ? 'SELL' : 'BUY',
    positionSide ?? null,
  );

  return signedRequest('POST', '/fapi/v1/order', params);
}

export async function placeFuturesTakeProfitLimit({
  symbol,
  side,
  stopPrice,
  price,
  quantity,
  timeInForce = 'GTC',
  reduceOnly = null,
  positionSide,
  newClientOrderId,
}) {
  const n = await normalizeOrder({ symbol, stopPrice, price, quantity });

  const base = {
    symbol,
    side,
    type: 'TAKE_PROFIT',
    timeInForce,
    stopPrice: n.stopPrice,
    price: n.price,
    quantity: n.quantity,
    workingType: 'MARK_PRICE',
    newClientOrderId,
    newOrderRespType: 'RESULT',
  };
  if (reduceOnly != null) base.reduceOnly = String(reduceOnly);

  const params = await withPositionSide(
    base,
    side === 'BUY' ? 'SELL' : 'BUY',
    positionSide ?? null,
  );

  return signedRequest('POST', '/fapi/v1/order', params);
}

export async function getFuturesOrder({ symbol, orderId, origClientOrderId }) {
  const params = { symbol };
  if (origClientOrderId) params.origClientOrderId = String(origClientOrderId);
  else if (orderId != null) params.orderId = String(orderId);
  else throw new Error('getFuturesOrder requires orderId or origClientOrderId');
  return signedRequest('GET', '/fapi/v1/order', params);
}

export async function cancelFuturesOrder({ symbol, orderId, origClientOrderId }) {
  const params = { symbol };
  if (origClientOrderId) params.origClientOrderId = String(origClientOrderId);
  else if (orderId != null) params.orderId = String(orderId);
  else throw new Error('cancelFuturesOrder requires orderId or origClientOrderId');
  return signedRequest('DELETE', '/fapi/v1/order', params);
}

export async function placeFuturesMarketOrder({ symbol, side, quantity, reduceOnly, positionSide, newClientOrderId }) {
  const n = await normalizeOrder({ symbol, quantity });
  const base = {
    symbol,
    side,
    type: 'MARKET',
    quantity: n.quantity,
    newClientOrderId,
    newOrderRespType: 'RESULT',
  };

  // Some testnet setups reject reduceOnly; only send it when explicitly set.
  if (reduceOnly != null) base.reduceOnly = String(reduceOnly);

  const params = await withPositionSide(base, side, positionSide ?? null);
  return signedRequest('POST', '/fapi/v1/order', params);
}

export async function getFuturesPositionRisk() {
  return signedRequest('GET', '/fapi/v2/positionRisk', {});
}

export async function getFuturesOpenOrders({ symbol }) {
  return signedRequest('GET', '/fapi/v1/openOrders', { symbol });
}

// --- Algo order fallback (some accounts require this for STOP/TP; error code -4120) ---
export async function placeFuturesAlgoOrder({ algoOrderPath, ...params }) {
  // Binance conditional orders migrated to Algo Service.
  // Default: POST /fapi/v1/algoOrder
  const path = algoOrderPath ?? '/fapi/v1/algoOrder';
  return signedRequest('POST', path, params);
}

export async function placeFuturesAlgoConditional({
  algoOrderPath,
  symbol,
  side,
  type, // STOP_MARKET | TAKE_PROFIT_MARKET
  triggerPrice,
  quantity,
  workingType = 'MARK_PRICE',
  reduceOnly = true,
}) {
  const n = await normalizeOrder({ symbol, stopPrice: triggerPrice, quantity });

  return placeFuturesAlgoOrder({
    algoOrderPath,
    algotype: 'CONDITIONAL',
    symbol,
    side,
    type,
    triggerprice: n.stopPrice,
    quantity: n.quantity,
    reduceOnly: reduceOnly ? 'true' : 'false',
    workingType,
  });
}

// Public (no-sign) endpoint: current mark price and index info
export async function getFuturesPremiumIndex({ symbol }) {
  const { data } = await binanceTradeHttp.get('/fapi/v1/premiumIndex', { params: { symbol } });
  return data;
}
