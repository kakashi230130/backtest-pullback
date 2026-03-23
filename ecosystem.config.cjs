// PM2 ecosystem for binance-candles-backend (Linux/VPS)
// Usage (on VPS):
//   pm2 start ecosystem.config.cjs
//   pm2 save
//   pm2 startup
// Notes:
// - This replaces Windows Task Scheduler .cmd jobs.
// - Sync candles are already scheduled by src/cron.js when you run `npm start`.
// - Analyze is scheduled by src/analyzeScheduler.js (10m + 35s).

const path = require('path');

const APP_DIR = process.env.BC_APP_DIR || __dirname; // allow override if you deploy elsewhere

module.exports = {
  apps: [
    // 1) Candle sync daemon (runs node-cron schedules for 5m/15m/30m/1h/4h/1d)
    {
      name: 'bc-sync-cron',
      cwd: APP_DIR,
      script: 'npm',
      args: 'run -s start',
      time: true,
      env: {
        NODE_ENV: 'production',
        // Must match your trading namespace
        EXCHANGE_NAME: 'binance_usdtm_mainnet',
        DB_POOL_LIMIT: '3',
        // long-running daemon: do NOT close the pool
        CLOSE_DB_POOL: '0',
      },
    },

    // 2) Analyze scheduler daemon (runs analyze every 10 minutes)
    {
      name: 'bc-analyze',
      cwd: APP_DIR,
      script: 'node',
      args: 'src/analyzeScheduler.js',
      time: true,
      env: {
        NODE_ENV: 'production',
        EXCHANGE_NAME: 'binance_usdtm_mainnet',

        // Profile
        // Use STRICT profile to filter only higher-quality setups (lower frequency)
        ANALYZE_PROFILE: 'strict',
        ANALYZE_EASY_MODE: '0',

        // STRICT SL/TP widening knobs (used in strict swing-based setup)
        SL_SWING_LOOKBACK: '20',
        SL_ATR_MULT: '0.35',
        TP_ATR_MULT: '0.7',
        MIN_RISK_PCT: '0.001',
        MIN_TP_PCT: '0.002',

        // SL/TP are placed by watcher after entry is FILLED
        PLACE_SLTP_ORDERS: '0',

        DB_POOL_LIMIT: '3',
        CLOSE_DB_POOL: '0',
      },
    },

    // 3) Order watcher burst daemon (places protective SL/TP after fill)
    {
      name: 'bc-watcher-burst',
      cwd: APP_DIR,
      script: 'node',
      args: 'src/orderWatcherBurst.js',
      time: true,
      env: {
        NODE_ENV: 'production',
        EXCHANGE_NAME: 'binance_usdtm_mainnet',

        // Protective placement
        PLACE_SLTP_ORDERS: '1',
        ENABLE_ACTIVE_BACKFILL: '0',

        // Algo endpoint config (if your account needs it)
        ALGO_SLTP_ENABLED: '1',
        BINANCE_ALGO_ORDER_PATH: '/fapi/v1/algoOrder',

        DB_POOL_LIMIT: '2',
        ORDER_WATCH_MS: '2000',
        ORDER_WATCH_BURST_MS: '55000',

        CLOSE_DB_POOL: '0',
      },
    },

    // --- Optional one-shot utilities (kept for parity with scripts/sync-*.cmd) ---
    // They exit immediately. Start manually when needed:
    //   pm2 start ecosystem.config.cjs --only bc-sync-5m-once
    //   pm2 logs bc-sync-5m-once
    // {
    //   name: 'bc-sync-5m-once',
    //   cwd: APP_DIR,
    //   script: 'npm',
    //   args: 'run -s sync:5m',
    //   autorestart: false,
    //   env: { NODE_ENV: 'production', EXCHANGE_NAME: 'binance_usdtm_mainnet', DB_POOL_LIMIT: '2', CLOSE_DB_POOL: '1' },
    // },
  ],
};
