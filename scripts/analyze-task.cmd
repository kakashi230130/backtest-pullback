@echo off
cd /d C:\Users\Admin\.openclaw\workspace\binance-candles-backend
set JOB_NAME=binance-analyze-30m

rem Run analyze against mainnet namespace (matches watcher)
set EXCHANGE_NAME=binance_usdtm_mainnet

rem Analyze profile
rem strict = old logic (rare trades)
rem scalp  = higher frequency
set ANALYZE_PROFILE=strict

rem Easy mode (pipeline test only)
set ANALYZE_EASY_MODE=0

rem ----- scalp tuning (target ~5-10 trades/day for ETHUSDT) -----
set SCALP_MIN_ADX=14
set SCALP_RSI5_BUY_MIN=51.5
set SCALP_RSI5_SELL_MAX=48.5
set SCALP_RSI15_BUY_MAX=70
set SCALP_RSI15_SELL_MIN=30
set SCALP_RISK_PCT=0.0015
set SCALP_TP_R_MULT=1.4
set SCALP_MIN_RR=1.3

rem SL/TP are placed by the watcher after entry FILLED
rem Keep analyze focused on entry + DB insert (avoid failing before DB insert)
set PLACE_SLTP_ORDERS=0

set DB_POOL_LIMIT=3
set CLOSE_DB_POOL=1
npm run -s analyze
