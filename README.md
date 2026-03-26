# crypto-backtest-minimal

Project tối giản để backtest logic `analyze` trực tiếp từ MySQL (bảng `candles`).

## Setup

```bash
cd backtest-minimal
npm i
copy .env.example .env
```

## Backfill dữ liệu (optional)

```bash
npm run backfill:range -- --symbol ETHUSDT --interval 5m --start "2025-01-01T00:00:00Z" --end "2025-12-31T23:59:59Z"
```

## Aggregate khung lớn từ 5m (optional)

```bash
npm run aggregate:from5m -- --symbol ETHUSDT --start "2025-01-01T00:00:00Z" --end "2025-12-31T23:59:59Z" --intervals "15m,30m,1h,4h,1d"
```

## Run backtest

```bash
npm run backtest -- --symbol ETHUSDT --start "2025-01-01T00:00:00Z" --end "2025-12-31T23:59:59Z" --out output/backtest_ETH_2025.json --debug 1
```

Output file có format:

```json
{ "summary": { }, "trades": [ ], "equity_curve": [ ] }
```
