import axios from 'axios';

export const binanceHttp = axios.create({
  baseURL: process.env.BINANCE_BASE_URL ?? 'https://fapi.binance.com',
  timeout: 30_000,
});

export function onlyClosedKlines(klines, nowMs = Date.now()) {
  return (klines ?? []).filter(k => Number(k?.[6]) < nowMs);
}

export async function fetchKlines({ symbol, interval, startTime, endTime, limit = 1000 }) {
  const params = { symbol, interval, limit };
  if (startTime !== undefined) params.startTime = startTime;
  if (endTime !== undefined) params.endTime = endTime;

  const { data } = await binanceHttp.get('/fapi/v1/klines', { params });
  return data;
}
