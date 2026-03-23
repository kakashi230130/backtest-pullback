@echo off
cd /d C:\Users\Admin\.openclaw\workspace\binance-candles-backend
set JOB_NAME=binance-sync-30m
set DB_POOL_LIMIT=2
npm run -s sync:30m
