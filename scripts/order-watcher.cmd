@echo off
cd /d C:\Users\Admin\.openclaw\workspace\binance-candles-backend
set DB_POOL_LIMIT=2
set ORDER_WATCH_MS=2000
node src\orderWatcher.js
