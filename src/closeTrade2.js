import { pool } from './db.js';

async function main() {
  const now = Date.now();
  const [res] = await pool.query(
    `UPDATE open_trades SET status='CLOSED', closed_at=:now, notes='MANUAL_CLOSE' WHERE id=2`,
    { now },
  );
  const [[t]] = await pool.query(`SELECT id,status,closed_at,notes FROM open_trades WHERE id=2`);
  console.log({ res, t });
  await pool.end();
}

main().catch(async e=>{console.error(e); try{await pool.end();}catch{} process.exit(1);});
