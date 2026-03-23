import { pool } from './db.js';

export function mapKline(symbol, interval, k) {
  // Binance kline array format:
  // 0 open time
  // 1 open
  // 2 high
  // 3 low
  // 4 close
  // 5 volume
  // 6 close time
  // 7 quote asset volume
  // 8 number of trades
  // 9 taker buy base asset volume
  // 10 taker buy quote asset volume
  // 11 ignore
  return {
    symbol,
    interval_code: interval,
    open_time: Number(k[0]),
    open: k[1],
    high: k[2],
    low: k[3],
    close: k[4],
    volume: k[5],
    close_time: Number(k[6]),
    quote_asset_volume: k[7],
    number_of_trades: Number(k[8]),
    taker_buy_base_asset_volume: k[9],
    taker_buy_quote_asset_volume: k[10],
    is_closed: 1,
    // indicators (nullable)
    rsi: null,
    ma20: null,
    ma50: null,
    ma200: null,
  };
}

export async function upsertCandles(rows) {
  if (!rows.length) return { insertedOrUpdated: 0 };

  // Bulk insert with ON DUPLICATE KEY UPDATE
  // Note: mysql2 supports array-of-arrays.
  const cols = [
    'symbol',
    'interval_code',
    'open_time',
    'open',
    'high',
    'low',
    'close',
    'volume',
    'close_time',
    'quote_asset_volume',
    'number_of_trades',
    'taker_buy_base_asset_volume',
    'taker_buy_quote_asset_volume',
    'is_closed',
    'rsi',
    'ma20',
    'ma50',
    'ma200',
  ];

  const values = rows.map(r => cols.map(c => r[c]));

  const sql = `
    INSERT INTO candles (${cols.join(',')})
    VALUES ${values.map(() => `(${cols.map(() => '?').join(',')})`).join(',')}
    ON DUPLICATE KEY UPDATE
      open=VALUES(open),
      high=VALUES(high),
      low=VALUES(low),
      close=VALUES(close),
      volume=VALUES(volume),
      close_time=VALUES(close_time),
      quote_asset_volume=VALUES(quote_asset_volume),
      number_of_trades=VALUES(number_of_trades),
      taker_buy_base_asset_volume=VALUES(taker_buy_base_asset_volume),
      taker_buy_quote_asset_volume=VALUES(taker_buy_quote_asset_volume),
      is_closed=VALUES(is_closed),
      rsi=COALESCE(VALUES(rsi), rsi),
      ma20=COALESCE(VALUES(ma20), ma20),
      ma50=COALESCE(VALUES(ma50), ma50),
      ma200=COALESCE(VALUES(ma200), ma200)
  `;

  const flat = values.flat();
  const [result] = await pool.query(sql, flat);
  return { insertedOrUpdated: result.affectedRows ?? 0 };
}

export async function getLastOpenTime(symbol, interval) {
  const [rows] = await pool.query(
    `SELECT open_time FROM candles WHERE symbol=:symbol AND interval_code=:interval ORDER BY open_time DESC LIMIT 1`,
    { symbol, interval },
  );
  return rows.length ? Number(rows[0].open_time) : null;
}

export async function getRecentClosesBefore(symbol, interval, beforeOpenTime, limit = 250) {
  const [rows] = await pool.query(
    `
    SELECT open_time, close
    FROM candles
    WHERE symbol=:symbol
      AND interval_code=:interval
      AND open_time < :beforeOpenTime
    ORDER BY open_time DESC
    LIMIT ${Number(limit) | 0}
    `,
    { symbol, interval, beforeOpenTime },
  );

  // rows are DESC; return ASC
  return rows
    .map(r => ({ open_time: Number(r.open_time), close: Number(r.close) }))
    .sort((a, b) => a.open_time - b.open_time);
}
