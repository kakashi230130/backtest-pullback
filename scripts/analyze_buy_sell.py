#!/usr/bin/env python3
"""Analyze BUY vs SELL performance from OpenClaw backtest JSON outputs.

Usage (Windows PowerShell):
  python .\scripts\analyze_buy_sell.py ..\output\result_BTC.json
  python .\scripts\analyze_buy_sell.py ..\output\result_ETH.json --pretty
  python .\scripts\analyze_buy_sell.py ..\output\*.json

The script tries to infer trade direction (BUY/SELL) using:
  1) trade.meta.setup.action
  2) trade.meta.analysis.bias
  3) trade.side (LONG->BUY, SHORT->SELL)

Wins/losses are based on netPnl > 0 (win), < 0 (loss). netPnl == 0 is counted as breakeven.
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass
class Agg:
    n: int = 0
    wins: int = 0
    losses: int = 0
    breakeven: int = 0
    sum_win: float = 0.0
    sum_loss: float = 0.0  # negative numbers

    def add(self, net_pnl: float) -> None:
        self.n += 1
        if net_pnl > 0:
            self.wins += 1
            self.sum_win += net_pnl
        elif net_pnl < 0:
            self.losses += 1
            self.sum_loss += net_pnl
        else:
            self.breakeven += 1

    def win_rate(self) -> float:
        if self.n == 0:
            return float("nan")
        return self.wins / self.n

    def avg_win(self) -> float:
        if self.wins == 0:
            return float("nan")
        return self.sum_win / self.wins

    def avg_loss(self) -> float:
        if self.losses == 0:
            return float("nan")
        return self.sum_loss / self.losses

    def avg_loss_abs(self) -> float:
        v = self.avg_loss()
        return float("nan") if math.isnan(v) else abs(v)


def _get_nested(d: Dict[str, Any], *path: str) -> Optional[Any]:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def infer_action(trade: Dict[str, Any]) -> Optional[str]:
    action = _get_nested(trade, "meta", "setup", "action")
    if isinstance(action, str) and action.strip():
        return action.strip().upper()

    bias = _get_nested(trade, "meta", "analysis", "bias")
    if isinstance(bias, str) and bias.strip():
        return bias.strip().upper()

    side = trade.get("side")
    if isinstance(side, str):
        side_u = side.strip().upper()
        if side_u == "LONG":
            return "BUY"
        if side_u == "SHORT":
            return "SELL"

    return None


def load_trades(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    trades = data.get("trades")
    if not isinstance(trades, list):
        raise ValueError(f"Expected key 'trades' to be a list in {path}")
    return trades  # type: ignore[return-value]


def analyze_file(path: str) -> Tuple[Agg, Agg, Agg, Dict[str, int]]:
    trades = load_trades(path)

    buy = Agg()
    sell = Agg()
    unknown = Agg()

    unknown_action_counts: Dict[str, int] = {}

    for t in trades:
        if not isinstance(t, dict):
            continue

        net = t.get("netPnl")
        if net is None:
            # fallback: grossPnl - fees
            gp = t.get("grossPnl")
            fees = t.get("fees")
            if isinstance(gp, (int, float)) and isinstance(fees, (int, float)):
                net = gp - fees

        if not isinstance(net, (int, float)):
            continue

        action = infer_action(t)
        if action == "BUY":
            buy.add(float(net))
        elif action == "SELL":
            sell.add(float(net))
        else:
            unknown.add(float(net))
            key = str(action) if action is not None else "<missing>"
            unknown_action_counts[key] = unknown_action_counts.get(key, 0) + 1

    total = Agg(
        n=buy.n + sell.n + unknown.n,
        wins=buy.wins + sell.wins + unknown.wins,
        losses=buy.losses + sell.losses + unknown.losses,
        breakeven=buy.breakeven + sell.breakeven + unknown.breakeven,
        sum_win=buy.sum_win + sell.sum_win + unknown.sum_win,
        sum_loss=buy.sum_loss + sell.sum_loss + unknown.sum_loss,
    )

    return buy, sell, total, unknown_action_counts


def fmt_float(x: float, digits: int = 4) -> str:
    if math.isnan(x):
        return "n/a"
    return f"{x:.{digits}f}"


def print_report(path: str, buy: Agg, sell: Agg, total: Agg, unknown_action_counts: Dict[str, int], pretty: bool) -> None:
    name = os.path.basename(path)

    def row(label: str, a: Agg) -> str:
        if pretty:
            return (
                f"{label:<7}  trades={a.n:<5} wins={a.wins:<5} losses={a.losses:<5} be={a.breakeven:<5} "
                f"win_rate={fmt_float(a.win_rate(), 4):>8}  avg_win={fmt_float(a.avg_win(), 4):>10}  "
                f"avg_loss={fmt_float(a.avg_loss(), 4):>10}  avg_loss_abs={fmt_float(a.avg_loss_abs(), 4):>10}"
            )
        else:
            return (
                f"{label} trades={a.n} wins={a.wins} losses={a.losses} be={a.breakeven} "
                f"win_rate={fmt_float(a.win_rate())} avg_win={fmt_float(a.avg_win())} avg_loss={fmt_float(a.avg_loss())}"
            )

    print(f"\n== {name} ==")
    print(row("BUY", buy))
    print(row("SELL", sell))
    print(row("TOTAL", total))

    if unknown_action_counts:
        top = sorted(unknown_action_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
        print("Unknown-direction trades breakdown (top):")
        for k, v in top:
            print(f"  {k}: {v}")


def expand_paths(inputs: List[str]) -> List[str]:
    out: List[str] = []
    for p in inputs:
        matches = glob.glob(p)
        if matches:
            out.extend(matches)
        else:
            out.append(p)
    # de-dup while preserving order
    seen = set()
    deduped = []
    for p in out:
        ap = os.path.abspath(p)
        if ap not in seen:
            seen.add(ap)
            deduped.append(ap)
    return deduped


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="+", help="JSON file(s) to analyze (supports glob)")
    parser.add_argument("--pretty", action="store_true", help="Align columns for easier reading")
    args = parser.parse_args()

    paths = expand_paths(args.paths)

    for p in paths:
        buy, sell, total, unknown_counts = analyze_file(p)
        print_report(p, buy, sell, total, unknown_counts, pretty=args.pretty)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
