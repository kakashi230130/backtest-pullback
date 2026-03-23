import axios from 'axios';
import { config } from './config.js';

export const binanceHttp = axios.create({
  baseURL: config.binance.baseUrl,
  timeout: 30_000,
  headers: config.binance.apiKey ? { 'X-MBX-APIKEY': config.binance.apiKey } : {},
});

export function onlyClosedKlines(klines, nowMs = Date.now()) {
  // Binance kline[6] is closeTime in ms.
  // The most recent candle returned by the API may still be forming (closeTime in the future).
  // To keep OHLC/RSI/MA consistent, we drop any candle whose closeTime is not yet reached.
  return (klines ?? []).filter(k => Number(k?.[6]) < nowMs);
}

export async function fetchKlines({ symbol, interval, startTime, endTime, limit = 1000 }) {
  const params = { symbol, interval, limit };
  if (startTime !== undefined) params.startTime = startTime;
  if (endTime !== undefined) params.endTime = endTime;

  const { data } = await binanceHttp.get('/fapi/v1/klines', { params });
  // data is array of arrays
  return data;
}
