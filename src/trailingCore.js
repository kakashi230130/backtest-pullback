function clamp(n, lo, hi) {
  const x = Number(n);
  if (!Number.isFinite(x)) return lo;
  return Math.min(hi, Math.max(lo, x));
}

export function maybeMoveStopLoss({ side, entry, initialSl, currentSl, price }) {
  const beEnabled = (process.env.AUTO_BREAKEVEN_ENABLED ?? '1') === '1';
  const trailEnabled = (process.env.TRAILING_STOP_ENABLED ?? '1') === '1';

  if (!beEnabled && !trailEnabled) return { newSl: null, reason: null, lockR: null };

  const e = Number(entry);
  const sl0 = Number(initialSl);
  const slNow = Number(currentSl);
  const p = Number(price);
  if (![e, sl0, slNow, p].every(Number.isFinite)) return { newSl: null, reason: null, lockR: null };

  const Rbase = side === 'LONG' ? (e - sl0) : (sl0 - e);
  if (!Number.isFinite(Rbase) || Rbase <= 0) return { newSl: null, reason: null, lockR: null };

  const favorable = side === 'LONG' ? (p - e) : (e - p);
  const favorableR = favorable / Rbase;

  if (beEnabled && favorableR >= Number(process.env.AUTO_BE_AT_R ?? 1)) {
    const beOffsetPct = Number(process.env.AUTO_BE_OFFSET_PCT ?? 0);
    const off = e * clamp(beOffsetPct, 0, 0.002);
    const bePrice = side === 'LONG' ? (e + off) : (e - off);

    const needsBE = side === 'LONG' ? slNow < bePrice : slNow > bePrice;
    if (needsBE) return { newSl: bePrice, reason: 'AUTO_BREAKEVEN', lockR: 0 };
  }

  if (trailEnabled && favorableR >= Number(process.env.TRAIL_START_R ?? 2)) {
    const lockR = Math.max(1, Math.floor(favorableR) - 1);
    const desired = side === 'LONG' ? (e + lockR * Rbase) : (e - lockR * Rbase);

    const needsTrail = side === 'LONG' ? slNow < desired : slNow > desired;
    if (needsTrail) return { newSl: desired, reason: `AUTO_TRAIL_LOCK_${lockR}R`, lockR };
  }

  return { newSl: null, reason: null, lockR: null };
}
