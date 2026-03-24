-- Add initial_sl column so trailing/breakeven logic can compute a stable R after process restarts.
--
-- Why: stop_loss can be moved to breakeven/trailing; if the watcher restarts and we recompute R from stop_loss,
-- R can become 0 and break trailing logic.

ALTER TABLE open_trades
  ADD COLUMN initial_sl DECIMAL(30,12) NULL AFTER take_profit;

-- Backfill for existing rows (best-effort):
UPDATE open_trades
  SET initial_sl = stop_loss
  WHERE initial_sl IS NULL AND stop_loss IS NOT NULL;
