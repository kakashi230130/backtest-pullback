@echo off
cd /d C:\Users\Admin\.openclaw\workspace\binance-candles-backend
set JOB_NAME=binance-order-watcher

rem Ensure watcher looks at the same exchange namespace as analyze/trading
rem (change to binance_usdtm_demo if you're intentionally using demo)
set EXCHANGE_NAME=binance_usdtm_mainnet

rem Enable protective SL/TP placement (watcher will place after entry FILLED)
set PLACE_SLTP_ORDERS=1

rem Do NOT backfill/create SL/TP for trades that are already ACTIVE
set ENABLE_ACTIVE_BACKFILL=0

rem Try algo endpoint mode again (Binance may require it for some accounts)
rem If it 404s again, set this back to 0.
set ALGO_SLTP_ENABLED=1

rem Correct algo endpoint candidate (previous /fapi/v1/algo/order returns 404)
rem We'll attempt Binance's SAPI futures algo endpoint.
set BINANCE_ALGO_ORDER_PATH=/fapi/v1/algoOrder

set DB_POOL_LIMIT=2
set ORDER_WATCH_MS=2000
set ORDER_WATCH_BURST_MS=55000
set CLOSE_DB_POOL=1
node src\orderWatcherBurst.js
