@echo off
cd /d C:\Users\Admin\.openclaw\workspace\binance-candles-backend
set JOB_NAME=binance-sync-4h
set DB_POOL_LIMIT=2
npm run -s sync:4h
