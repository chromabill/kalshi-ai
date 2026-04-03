"""
Auto Swing Trader — buys the underdog when they fall behind in elite NBA matchups,
sells when the price swings back up.

Strategy:
- Wait for game to go live
- Buy the underdog when they're down 5+ points and price drops below entry target
- Sell when price rises to profit target
- Small positions only ($10 max per trade)
- Can do multiple swings per game if the lead keeps changing
"""

import asyncio
import uuid
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import httpx

KALSHI_BASE = "https://api.elections.kalshi.com"
ESPN_NBA = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
STATE_PATH = Path(__file__).parent / "swing_state.json"

TEAM_MAP = {
    "Timberwolves": "MIN", "Pistons": "DET", "Suns": "PHX", "Hornets": "CHA",
    "Lakers": "LAL", "Thunder": "OKC", "Cavaliers": "CLE", "Warriors": "GSW",
    "Pelicans": "NOP", "Trail Blazers": "POR", "Spurs": "SA", "Clippers": "LAC",
    "Celtics": "BOS", "Heat": "MIA", "Bucks": "MIL", "Rockets": "HOU",
    "Nets": "BKN", "Knicks": "NYK", "76ers": "PHI", "Raptors": "TOR",
    "Bulls": "CHI", "Pacers": "IND", "Hawks": "ATL", "Magic": "ORL",
    "Wizards": "WAS", "Mavericks": "DAL", "Grizzlies": "MEM", "Kings": "SAC",
    "Nuggets": "DEN", "Jazz": "UTA",
}


@dataclass
class SwingTrade:
    ticker: str
    team: str
    side: str
    contracts: int
    entry_price: float
    cost: float
    target_sell: float
    status: str = "open"  # open, sold, expired
    sell_price: float = 0
    pnl: float = 0
    opened_at: str = ""
    closed_at: str = ""


@dataclass
class GameWatch:
    """A game being watched for swing opportunities."""
    away_abbr: str
    home_abbr: str
    away_team: str
    home_team: str
    date_str: str  # Kalshi date like "26APR02"
    max_budget: float = 10.0
    max_entry_price: float = 0.25  # only buy below this
    min_score_diff: int = 5  # underdog must be down at least this much
    profit_target_pct: float = 0.40  # sell when price is 40% above entry
    max_swings: int = 3  # max trades per game
    swings_done: int = 0


class SwingTrader:
    """Watches games and executes swing trades automatically."""

    def __init__(self, api_key: str, private_key_path: str):
        self.api_key = api_key
        self.private_key_path = private_key_path
        self._http: Optional[httpx.AsyncClient] = None
        self.watches: List[GameWatch] = []
        self.trades: List[SwingTrade] = []
        self.log: List[str] = []

    async def start(self):
        self._http = httpx.AsyncClient(timeout=15.0)

    async def stop(self):
        if self._http:
            await self._http.aclose()

    def watch_game(self, watch: GameWatch):
        """Add a game to watch for swing opportunities."""
        self.watches.append(watch)
        self.log.append(f"Watching {watch.away_team} @ {watch.home_team}")
        print(f"\n  👀 SWING TRADER: Watching {watch.away_team} @ {watch.home_team}")
        print(f"     Buy under ${watch.max_entry_price:.2f} when down {watch.min_score_diff}+")
        print(f"     Sell at +{watch.profit_target_pct:.0%} profit")
        print(f"     Budget: ${watch.max_budget:.0f}, max {watch.max_swings} swings")

    async def check(self):
        """Check all watched games for swing opportunities."""
        if not self.watches:
            return

        # Get live scores
        try:
            resp = await self._http.get(ESPN_NBA)
            if resp.status_code != 200:
                return
            espn_data = resp.json()
        except Exception:
            return

        for watch in self.watches:
            await self._check_game(watch, espn_data)

        # Check open trades for sell targets
        await self._check_sells()

    async def _check_game(self, watch: GameWatch, espn_data: dict):
        """Check a single game for buy opportunities."""
        if watch.swings_done >= watch.max_swings:
            return

        # Check if we already have an open trade for this game
        open_trades = [t for t in self.trades if t.status == "open"
                       and watch.away_abbr in t.ticker]
        if open_trades:
            return  # Wait for current trade to close before opening another

        # Find this game in ESPN data
        game = None
        for event in espn_data.get("events", []):
            status = event["status"]["type"]["description"]
            if status not in ("In Progress", "End of Period", "Halftime", "Scheduled"):
                continue

            competitors = event["competitions"][0]["competitors"]
            team_names = [c["team"]["shortDisplayName"] for c in competitors]

            away_name = None
            home_name = None
            for c in competitors:
                name = c["team"]["shortDisplayName"]
                abbr = TEAM_MAP.get(name, "")
                if abbr == watch.away_abbr:
                    away_name = name
                if abbr == watch.home_abbr:
                    home_name = name

            if away_name and home_name:
                scores = {}
                for c in competitors:
                    scores[c["team"]["shortDisplayName"]] = int(c.get("score", "0") or "0")
                period = event["status"].get("period", 0)
                game = {
                    "away_score": scores.get(away_name, 0),
                    "home_score": scores.get(home_name, 0),
                    "period": period,
                    "scheduled": status == "Scheduled",
                }
                break

        if not game:
            return

        away_score = game["away_score"]
        home_score = game["home_score"]
        period = game["period"]
        is_scheduled = game.get("scheduled", False)

        # Don't trade in Q4 (too risky for swings)
        if period >= 4:
            return

        # Pre-game: buy the pre-game underdog (cheaper side)
        if is_scheduled or (period == 0 and away_score == 0 and home_score == 0):
            # Buy whichever team is cheaper on Kalshi (the underdog)
            away_ticker = f"KXNBAGAME-{watch.date_str}{watch.away_abbr}{watch.home_abbr}-{watch.away_abbr}"
            home_ticker = f"KXNBAGAME-{watch.date_str}{watch.away_abbr}{watch.home_abbr}-{watch.home_abbr}"

            away_price = 0
            home_price = 0
            try:
                r1 = await self._http.get(f"{KALSHI_BASE}/trade-api/v2/markets/{away_ticker}")
                if r1.status_code == 200:
                    away_price = float(r1.json().get("market", r1.json()).get("yes_ask_dollars", "0") or "0")
                r2 = await self._http.get(f"{KALSHI_BASE}/trade-api/v2/markets/{home_ticker}")
                if r2.status_code == 200:
                    home_price = float(r2.json().get("market", r2.json()).get("yes_ask_dollars", "0") or "0")
            except Exception:
                return

            if away_price > 0 and away_price <= watch.max_entry_price:
                underdog_abbr = watch.away_abbr
                underdog_team = watch.away_team
                ask_price = away_price
                ticker = away_ticker
            elif home_price > 0 and home_price <= watch.max_entry_price:
                underdog_abbr = watch.home_abbr
                underdog_team = watch.home_team
                ask_price = home_price
                ticker = home_ticker
            else:
                return  # neither side is cheap enough

            diff = 0  # pre-game, no score diff
            # Skip to buy logic below
            contracts = max(1, int(watch.max_budget / ask_price))
            cost = contracts * ask_price
            target_sell = round(ask_price * (1 + watch.profit_target_pct), 2)

            from src.clients.kalshi_client import KalshiClient
            try:
                client = KalshiClient(api_key=self.api_key, private_key_path=self.private_key_path)
                order = await client.place_order(
                    ticker=ticker,
                    client_order_id=str(uuid.uuid4()),
                    side="yes", action="buy", count=contracts,
                    type_="limit", yes_price=int(ask_price * 100),
                )
                await client.close()

                fill_count = float(order.get("order", {}).get("fill_count_fp", "0") or "0")
                if fill_count <= 0:
                    return

                trade = SwingTrade(
                    ticker=ticker, team=underdog_team, side="yes",
                    contracts=int(fill_count), entry_price=ask_price,
                    cost=round(fill_count * ask_price, 2),
                    target_sell=target_sell,
                    opened_at=datetime.now(timezone.utc).isoformat(),
                )
                self.trades.append(trade)
                watch.swings_done += 1

                msg = (f"PRE-GAME BUY: {underdog_team} — "
                       f"{int(fill_count)} contracts @ ${ask_price:.2f} = ${trade.cost:.2f} "
                       f"(target sell ${target_sell:.2f})")
                self.log.append(msg)
                print(f"\n  🎯 {msg}")
            except Exception as e:
                self.log.append(f"Pre-game buy failed: {e}")
            return

        # --- Live game logic ---

        # Who's the underdog right now?
        if away_score < home_score:
            underdog_abbr = watch.away_abbr
            underdog_team = watch.away_team
            diff = home_score - away_score
        elif home_score < away_score:
            underdog_abbr = watch.home_abbr
            underdog_team = watch.home_team
            diff = away_score - home_score
        else:
            return  # tied

        if diff < watch.min_score_diff:
            return

        # Check Kalshi price
        ticker = f"KXNBAGAME-{watch.date_str}{watch.away_abbr}{watch.home_abbr}-{underdog_abbr}"
        try:
            mr = await self._http.get(f"{KALSHI_BASE}/trade-api/v2/markets/{ticker}")
            if mr.status_code != 200:
                return
            market = mr.json().get("market", mr.json())
            ask_price = float(market.get("yes_ask_dollars", "0") or "0")
        except Exception:
            return

        if ask_price <= 0 or ask_price > watch.max_entry_price:
            return

        # BUY!
        contracts = max(1, int(watch.max_budget / ask_price))
        cost = contracts * ask_price
        target_sell = round(ask_price * (1 + watch.profit_target_pct), 2)

        from src.clients.kalshi_client import KalshiClient
        try:
            client = KalshiClient(api_key=self.api_key, private_key_path=self.private_key_path)
            order = await client.place_order(
                ticker=ticker,
                client_order_id=str(uuid.uuid4()),
                side="yes",
                action="buy",
                count=contracts,
                type_="limit",
                yes_price=int(ask_price * 100),
            )
            await client.close()

            fill_count = float(order.get("order", {}).get("fill_count_fp", "0") or "0")
            if fill_count <= 0:
                self.log.append(f"Order resting (not filled yet): {ticker}")
                return

            trade = SwingTrade(
                ticker=ticker, team=underdog_team, side="yes",
                contracts=int(fill_count), entry_price=ask_price,
                cost=round(fill_count * ask_price, 2),
                target_sell=target_sell,
                opened_at=datetime.now(timezone.utc).isoformat(),
            )
            self.trades.append(trade)
            watch.swings_done += 1

            msg = (f"SWING BUY: {underdog_team} ({ticker}) — "
                   f"{int(fill_count)} contracts @ ${ask_price:.2f} = ${trade.cost:.2f} "
                   f"(down {diff}, target sell ${target_sell:.2f})")
            self.log.append(msg)
            print(f"\n  🎯 {msg}")

        except Exception as e:
            self.log.append(f"Buy failed: {e}")
            print(f"  ❌ Swing buy failed: {e}")

    async def _check_sells(self):
        """Check open trades and sell if target hit."""
        for trade in self.trades:
            if trade.status != "open":
                continue

            try:
                mr = await self._http.get(f"{KALSHI_BASE}/trade-api/v2/markets/{trade.ticker}")
                if mr.status_code != 200:
                    continue
                market = mr.json().get("market", mr.json())
                bid = float(market.get("yes_bid_dollars", "0") or "0")
                status = market.get("status", "")
            except Exception:
                continue

            # Sell if target hit
            if bid >= trade.target_sell:
                from src.clients.kalshi_client import KalshiClient
                try:
                    client = KalshiClient(api_key=self.api_key, private_key_path=self.private_key_path)
                    order = await client.place_order(
                        ticker=trade.ticker,
                        client_order_id=str(uuid.uuid4()),
                        side="yes",
                        action="sell",
                        count=trade.contracts,
                        type_="limit",
                        yes_price=int(bid * 100),
                    )
                    await client.close()

                    revenue = trade.contracts * bid
                    fees = trade.contracts * 0.0365
                    trade.sell_price = bid
                    trade.pnl = round(revenue - trade.cost - fees, 2)
                    trade.status = "sold"
                    trade.closed_at = datetime.now(timezone.utc).isoformat()

                    msg = (f"SWING SELL: {trade.team} — {trade.contracts} @ ${bid:.2f} "
                           f"P&L: ${trade.pnl:+.2f}")
                    self.log.append(msg)
                    print(f"\n  💰 {msg}")

                except Exception as e:
                    self.log.append(f"Sell failed: {e}")

            # Market settled or dead
            elif status in ("settled", "finalized", "closed"):
                result = market.get("result", "")
                if result == "yes":
                    trade.pnl = round(trade.contracts * 1.0 - trade.cost, 2)
                    trade.status = "sold"
                else:
                    trade.pnl = round(-trade.cost, 2)
                    trade.status = "expired"
                trade.closed_at = datetime.now(timezone.utc).isoformat()
                self.log.append(f"SETTLED: {trade.team} — {trade.status} P&L: ${trade.pnl:+.2f}")

    def get_status(self) -> dict:
        return {
            "watching": [
                {"away": w.away_team, "home": w.home_team,
                 "budget": w.max_budget, "entry_target": w.max_entry_price,
                 "swings_done": w.swings_done, "max_swings": w.max_swings}
                for w in self.watches
            ],
            "open_trades": [
                {"team": t.team, "contracts": t.contracts,
                 "entry": t.entry_price, "target": t.target_sell,
                 "cost": t.cost}
                for t in self.trades if t.status == "open"
            ],
            "completed_trades": [
                {"team": t.team, "entry": t.entry_price,
                 "sell": t.sell_price, "pnl": t.pnl, "status": t.status}
                for t in self.trades if t.status != "open"
            ],
            "log": self.log[-15:],
        }
