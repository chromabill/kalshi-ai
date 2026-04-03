"""
Auto Position Manager — watches live positions and executes sell rules.

Rules:
- Sell half at 2x (lock in profit, let rest ride free)
- Trailing stop: sell remaining if price drops 15% from peak
- Applies to ALL positions automatically
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

import httpx

KALSHI_BASE = "https://api.elections.kalshi.com"
ESPN_NBA = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_NHL = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard"
STATE_PATH = Path(__file__).parent / "position_state.json"

# Map Kalshi ticker fragments to ESPN team names for matching
KALSHI_TO_TEAM = {
    "MIN": "Timberwolves", "DET": "Pistons", "PHX": "Suns", "CHA": "Hornets",
    "LAL": "Lakers", "OKC": "Thunder", "CLE": "Cavaliers", "GSW": "Warriors",
    "BOS": "Bruins", "FLA": "Panthers", "PIT": "Penguins", "TB": "Lightning",
    "BUF": "Sabres", "OTT": "Senators", "MTL": "Canadiens", "NYR": "Rangers",
    "CAR": "Hurricanes", "CBJ": "Blue Jackets", "WAS": "Capitals", "NJ": "Devils",
}


@dataclass
class PositionState:
    """Tracks state for auto-management of a single position."""
    ticker: str
    entry_price: float        # what we paid per contract
    total_contracts: float
    half_sold: bool = False   # have we sold half yet?
    contracts_remaining: float = 0
    peak_price: float = 0.0   # highest price seen (for trailing stop)
    sells_executed: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "entry_price": self.entry_price,
            "total_contracts": self.total_contracts,
            "half_sold": self.half_sold,
            "contracts_remaining": self.contracts_remaining,
            "peak_price": self.peak_price,
            "sells_executed": self.sells_executed,
        }


class AutoManager:
    """
    Watches all Kalshi positions and auto-sells based on rules:
    1. Sell half when price hits 2x entry (lock in cost basis)
    2. Trailing stop on remainder: sell if price drops 15% from peak
    """

    TAKE_PROFIT_MULTIPLIER = 2.0   # sell half at 2x
    TRAILING_STOP_PCT = 0.15       # sell rest if 15% drop from peak
    LATE_GAME_SELL_PRICE = 0.25    # if losing & price below this late in game, sell to salvage

    def __init__(self, api_key: str, private_key_path: str):
        self.api_key = api_key
        self.private_key_path = private_key_path
        self._http: Optional[httpx.AsyncClient] = None
        self.states: Dict[str, PositionState] = {}
        self.log: list = []
        self._load_state()

    async def start(self):
        self._http = httpx.AsyncClient(timeout=15.0)
        print("[auto-manager] Started — quarter-aware rules:")
        print("  Q1-Q2: No auto-sells (too early)")
        print("  Q3: Sell half at $0.80")
        print("  Q4: Sell half at $0.70")
        print("  Q4 < 2min: Sell all if profitable")
        print("  Salvage: Sell if price drops below $0.15")

    async def stop(self):
        self._save_state()
        if self._http:
            await self._http.aclose()

    async def _get_game_state(self, ticker: str) -> Tuple[int, str, bool]:
        """Get quarter/period, clock, and whether our side is winning from ESPN.

        Returns: (period, clock_str, is_winning)
        """
        # Figure out sport and teams from ticker
        is_nba = "KXNBA" in ticker
        is_nhl = "KXNHL" in ticker
        if not is_nba and not is_nhl:
            return (0, "", False)

        url = ESPN_NBA if is_nba else ESPN_NHL

        try:
            resp = await self._http.get(url)
            if resp.status_code != 200:
                return (0, "", False)
            data = resp.json()

            # Extract team abbrevs from ticker
            # e.g. KXNBAGAME-26APR02MINDET-MIN -> our_side=MIN, teams=MINDET
            parts = ticker.split("-")
            our_side = parts[-1] if len(parts) >= 3 else ""
            our_team_name = KALSHI_TO_TEAM.get(our_side, "")

            for event in data.get("events", []):
                competitors = event["competitions"][0]["competitors"]
                team_names = [c["team"]["shortDisplayName"] for c in competitors]

                if our_team_name not in team_names:
                    continue

                period = event["status"].get("period", 0)
                clock = event["status"].get("displayClock", "")
                status_desc = event["status"]["type"]["description"]

                if status_desc in ("Final", "Scheduled"):
                    return (period, clock, False)

                # Check if our team is winning
                our_score = 0
                opp_score = 0
                for c in competitors:
                    score = int(c.get("score", "0") or "0")
                    if c["team"]["shortDisplayName"] == our_team_name:
                        our_score = score
                    else:
                        opp_score = score

                is_winning = our_score > opp_score
                return (period, clock, is_winning)

        except Exception:
            pass
        return (0, "", False)

    async def check_positions(self):
        """Main loop iteration: fetch positions, check rules, execute sells."""
        from src.clients.kalshi_client import KalshiClient

        try:
            client = KalshiClient(
                api_key=self.api_key,
                private_key_path=self.private_key_path,
            )
            positions = await client.get_positions()
            market_positions = positions.get("market_positions", [])

            for p in market_positions:
                ticker = p.get("ticker", "")
                contracts = float(p.get("position_fp", "0") or "0")
                exposure = float(p.get("market_exposure_dollars", "0") or "0")

                if contracts <= 0:
                    continue

                fees = float(p.get("fees_paid_dollars", "0") or "0")
                # True cost per contract includes fees
                entry_price = (exposure + fees) / contracts if contracts > 0 else 0

                # Get current market price
                try:
                    resp = await self._http.get(f"{KALSHI_BASE}/trade-api/v2/markets/{ticker}")
                    if resp.status_code != 200:
                        continue
                    market = resp.json().get("market", resp.json())
                    current_bid = float(market.get("yes_bid_dollars", "0") or "0")
                    status = market.get("status", "")
                except Exception:
                    continue

                if status != "active" or current_bid <= 0:
                    continue

                # Init or update state
                if ticker not in self.states:
                    self.states[ticker] = PositionState(
                        ticker=ticker,
                        entry_price=entry_price,
                        total_contracts=contracts,
                        contracts_remaining=contracts,
                        peak_price=current_bid,
                    )

                state = self.states[ticker]
                state.contracts_remaining = contracts

                # Update peak price
                if current_bid > state.peak_price:
                    state.peak_price = current_bid

                # --- Get live game state from ESPN ---
                period, clock, is_winning = await self._get_game_state(ticker)

                # Parse clock minutes for Q4 late-game check
                clock_mins = 99
                try:
                    if ":" in clock:
                        clock_mins = float(clock.split(":")[0])
                    elif clock:
                        clock_mins = float(clock)
                except Exception:
                    pass

                # Is this position profitable (after fees)?
                sell_fees = contracts * 0.0365
                net_value = contracts * current_bid - sell_fees
                total_cost = exposure + float(p.get("fees_paid_dollars", "0") or "0")
                is_profitable = net_value > total_cost

                # --- Quarter-aware sell rules ---

                # Q1-Q2: No auto-sells (too early, let it play out)
                if period <= 2:
                    pass  # do nothing

                # Q3: Sell half if price hits $0.80 (strong lead)
                elif period == 3:
                    if not state.half_sold and current_bid >= 0.80:
                        half = int(contracts / 2)
                        if half > 0:
                            success = await self._sell(client, ticker, half, int(current_bid * 100))
                            if success:
                                state.half_sold = True
                                profit = half * current_bid
                                state.sells_executed.append({
                                    "type": "q3_take_profit",
                                    "contracts": half, "price": current_bid,
                                    "profit": round(profit, 2),
                                    "time": datetime.now(timezone.utc).isoformat(),
                                })
                                self.log.append(f"Q3 SELL HALF: {ticker} — {half} @ ${current_bid:.2f}")
                                print(f"\n  💰 Q3 SELL HALF: {ticker}")
                                print(f"     Sold {half} @ ${current_bid:.2f} = ${profit:.2f}")

                # Q4: Sell half at $0.70 / Sell ALL if profitable with < 2 min left
                elif period >= 4:
                    # Sell half at $0.70
                    if not state.half_sold and current_bid >= 0.70:
                        half = int(contracts / 2)
                        if half > 0:
                            success = await self._sell(client, ticker, half, int(current_bid * 100))
                            if success:
                                state.half_sold = True
                                profit = half * current_bid
                                state.sells_executed.append({
                                    "type": "q4_take_profit",
                                    "contracts": half, "price": current_bid,
                                    "profit": round(profit, 2),
                                    "time": datetime.now(timezone.utc).isoformat(),
                                })
                                self.log.append(f"Q4 SELL HALF: {ticker} — {half} @ ${current_bid:.2f}")
                                print(f"\n  💰 Q4 SELL HALF: {ticker}")
                                print(f"     Sold {half} @ ${current_bid:.2f} = ${profit:.2f}")

                    # Under 2 minutes left — sell everything if profitable
                    if clock_mins < 2 and is_profitable:
                        remaining = int(contracts)
                        if remaining > 0:
                            success = await self._sell(client, ticker, remaining, int(current_bid * 100))
                            if success:
                                profit = remaining * current_bid
                                state.sells_executed.append({
                                    "type": "q4_close_out",
                                    "contracts": remaining, "price": current_bid,
                                    "profit": round(profit, 2),
                                    "time": datetime.now(timezone.utc).isoformat(),
                                })
                                self.log.append(f"Q4 CLOSE OUT: {ticker} — {remaining} @ ${current_bid:.2f} (<2min left)")
                                print(f"\n  🏁 Q4 CLOSE OUT: {ticker}")
                                print(f"     Sold {remaining} @ ${current_bid:.2f} — locking in profit before buzzer")

                # --- Salvage rule (any quarter) ---
                # If price drops below $0.10, sell to recover scraps
                if current_bid <= 0.10 and current_bid > 0.01:
                    remaining = int(contracts)
                    if remaining > 0:
                        success = await self._sell(client, ticker, remaining, int(current_bid * 100))
                        if success:
                            salvage = remaining * current_bid
                            state.sells_executed.append({
                                "type": "salvage",
                                "contracts": remaining, "price": current_bid,
                                "salvaged": round(salvage, 2),
                                "time": datetime.now(timezone.utc).isoformat(),
                            })
                            self.log.append(f"SALVAGE: {ticker} — {remaining} @ ${current_bid:.2f}")
                            print(f"\n  🆘 SALVAGE: {ticker}")
                            print(f"     Sold {remaining} @ ${current_bid:.2f} — salvaged ${salvage:.2f}")

            await client.close()
            self._save_state()

        except Exception as e:
            print(f"[auto-manager] error: {e}")

    async def _sell(self, client, ticker: str, count: int, price_cents: int) -> bool:
        """Execute a sell order."""
        import uuid
        try:
            order = await client.place_order(
                ticker=ticker,
                client_order_id=str(uuid.uuid4()),
                side="yes",
                action="sell",
                count=count,
                type_="limit",
                yes_price=price_cents,
            )
            return order.get("order", {}).get("status") in ("executed", "resting")
        except Exception as e:
            print(f"  [auto-manager] sell failed: {e}")
            return False

    def get_status(self) -> dict:
        return {
            "active": True,
            "rules": {
                "take_profit": f"Sell half at {self.TAKE_PROFIT_MULTIPLIER}x",
                "trailing_stop": f"Sell rest if {self.TRAILING_STOP_PCT:.0%} drop from peak",
            },
            "positions": {
                ticker: {
                    "entry_price": s.entry_price,
                    "peak_price": s.peak_price,
                    "half_sold": s.half_sold,
                    "contracts_remaining": s.contracts_remaining,
                    "sells": s.sells_executed,
                }
                for ticker, s in self.states.items()
            },
            "recent_log": self.log[-10:],
        }

    def _save_state(self):
        data = {k: v.to_dict() for k, v in self.states.items()}
        STATE_PATH.write_text(json.dumps(data, indent=2))

    def _load_state(self):
        if STATE_PATH.exists():
            try:
                data = json.loads(STATE_PATH.read_text())
                for ticker, d in data.items():
                    self.states[ticker] = PositionState(
                        ticker=d["ticker"],
                        entry_price=d["entry_price"],
                        total_contracts=d["total_contracts"],
                        half_sold=d.get("half_sold", False),
                        contracts_remaining=d.get("contracts_remaining", 0),
                        peak_price=d.get("peak_price", 0),
                        sells_executed=d.get("sells_executed", []),
                    )
            except Exception:
                pass
