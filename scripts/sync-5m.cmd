@echo off
cd /d C:\Users\Admin\.openclaw\workspace\binance-candles-backend
set JOB_NAME=binance-sync-5m
set DB_POOL_LIMIT=2
npm run -s sync:5m
