export const intervals = {
  '5m': 5 * 60_000,
  '15m': 15 * 60_000,
  '30m': 30 * 60_000,
  '1h': 60 * 60_000,
  '4h': 4 * 60 * 60_000,
  '1d': 24 * 60 * 60_000,
};

export function intervalToMs(interval) {
  const ms = intervals[interval];
  if (!ms) throw new Error(`Unsupported interval: ${interval}`);
  return ms;
}
