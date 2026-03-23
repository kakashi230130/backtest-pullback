# binance-candles-backend

Node.js service to fetch **Binance candlesticks (klines)** and store them in **MySQL**.

## Features

- Stores candles for intervals: `5m`, `15m`, `30m`, `1h`, `4h`, `1d`
- Upsert by unique key `(symbol, interval_code, open_time)`
- Computes/stores indicators: RSI(14), MA20/50/200
- Cron scheduler inside the app (node-cron)
- Has a table `open_trades` for USDT-M futures trades (manual insert for now)

## 1) Setup

```bash
cd binance-candles-backend
cp .env.example .env
npm i
```

Edit `.env`:

- `DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME`
- `SYMBOLS=BTCUSDT,ETHUSDT`

Create database (example):

```sql
CREATE DATABASE binance_candles CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

Run migration (creates table + adds indicator columns):

```bash
npm run migrate
```

## 2) Run

Start cron service:

```bash
npm start
```

Manual one-shot sync (useful for testing):

```bash
npm run sync:5m
npm run sync:1h
```

Backfill ~200 newest candles per interval (and compute RSI/MA):

```bash
npm run backfill:latest200
```

Run market analysis + auto manage `open_trades` (one-shot):

```bash
npm run analyze
```

Run market analysis every 30 minutes (daemon):

```bash
npm run analyze:30m
```

## Notes

- Binance public endpoint used: `GET /api/v3/klines`
- This project intentionally **does not** do unlimited historical backfill on first run; it fetches the latest page (up to 1000 candles) per symbol/interval. You can extend `syncCandles.js` for deeper backfill.
