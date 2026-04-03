"""
NBA Swing Scanner — finds close games between good teams where one side is temporarily cheap.

Strategy:
1. Find NBA games where both teams have strong records (45+ wins)
2. During live games, buy whichever side is down early (cheap contracts)
3. Sell when the lead swings and price bumps up
4. Don't wait for game end — profit from the back-and-forth

This works because good teams trade leads constantly. A $0.30 contract
can jump to $0.50+ in one quarter when the losing team goes on a run.
"""

import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import httpx

ESPN_NBA = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
KALSHI_BASE = "https://api.elections.kalshi.com"

# ESPN team name → Kalshi abbreviation
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
class SwingOpportunity:
    """A detected swing trading opportunity."""
    # Game info
    away_team: str
    home_team: str
    away_score: int
    home_score: int
    period: int
    clock: str
    away_record: str
    home_record: str
    away_wins: int
    home_wins: int

    # Market info
    underdog_ticker: str  # ticker for the team that's currently losing
    underdog_team: str
    underdog_price: float  # current price to buy
    favorite_price: float
    market_volume: float

    # Signal quality
    score_diff: int  # how many points the underdog is down
    quality: str  # "great", "good", "watch"
    reason: str

    def to_dict(self) -> dict:
        return {
            "away_team": self.away_team,
            "home_team": self.home_team,
            "away_score": self.away_score,
            "home_score": self.home_score,
            "period": self.period,
            "clock": self.clock,
            "away_record": self.away_record,
            "home_record": self.home_record,
            "underdog_ticker": self.underdog_ticker,
            "underdog_team": self.underdog_team,
            "underdog_price": self.underdog_price,
            "favorite_price": self.favorite_price,
            "score_diff": self.score_diff,
            "quality": self.quality,
            "reason": self.reason,
            "potential_profit_pct": round((0.50 / max(self.underdog_price, 0.01) - 1) * 100, 0),
        }


class SwingScanner:
    """Scans for NBA swing trading opportunities."""

    # Only consider teams with this many wins (strong teams)
    MIN_WINS = 35

    def __init__(self):
        self._http: Optional[httpx.AsyncClient] = None
        self.opportunities: List[SwingOpportunity] = []

    async def start(self):
        self._http = httpx.AsyncClient(timeout=15.0)

    async def stop(self):
        if self._http:
            await self._http.aclose()

    async def scan(self) -> List[SwingOpportunity]:
        """Find swing opportunities in live NBA games."""
        opps = []

        try:
            resp = await self._http.get(ESPN_NBA)
            if resp.status_code != 200:
                return opps
            data = resp.json()
        except Exception as e:
            print(f"[swing] ESPN error: {e}")
            return opps

        for event in data.get("events", []):
            status = event["status"]["type"]["description"]
            if status not in ("In Progress", "End of Period", "Halftime", "Scheduled"):
                continue

            is_upcoming = status == "Scheduled"
            period = event["status"].get("period", 0)
            clock = event["status"].get("displayClock", "")

            comp = event["competitions"][0]
            teams = {}
            for c in comp.get("competitors", []):
                ha = c.get("homeAway", "")
                record = c.get("records", [{}])[0].get("summary", "0-0") if c.get("records") else "0-0"
                wins = int(record.split("-")[0]) if "-" in record else 0
                teams[ha] = {
                    "name": c["team"]["shortDisplayName"],
                    "abbr": TEAM_MAP.get(c["team"]["shortDisplayName"], ""),
                    "score": int(c.get("score", "0") or "0"),
                    "record": record,
                    "wins": wins,
                }

            away = teams.get("away", {})
            home = teams.get("home", {})

            if not away.get("abbr") or not home.get("abbr"):
                continue

            # Both teams must be decent
            if away["wins"] < self.MIN_WINS or home["wins"] < self.MIN_WINS:
                continue

            date_str = self._get_date_str(event)

            # --- Upcoming games: show as "watch" so user can plan ---
            if is_upcoming:
                both_elite = away["wins"] >= 50 and home["wins"] >= 50
                both_strong = away["wins"] >= 45 and home["wins"] >= 45

                if not both_strong:
                    continue

                # Get pre-game prices for the away team (usually the underdog on the road)
                away_ticker = f"KXNBAGAME-{date_str}{away['abbr']}{home['abbr']}-{away['abbr']}"
                home_ticker = f"KXNBAGAME-{date_str}{away['abbr']}{home['abbr']}-{home['abbr']}"
                away_price = 0
                home_price = 0
                vol = 0
                for tk, is_away in [(away_ticker, True), (home_ticker, False)]:
                    try:
                        mr = await self._http.get(f"{KALSHI_BASE}/trade-api/v2/markets/{tk}")
                        if mr.status_code == 200:
                            md = mr.json().get("market", md.json()) if False else mr.json().get("market", mr.json())
                            p = float(md.get("yes_bid_dollars", "0") or "0")
                            v = float(md.get("volume_fp", "0") or "0")
                            if is_away:
                                away_price = p
                            else:
                                home_price = p
                            vol = max(vol, v)
                    except Exception:
                        pass

                # Determine pre-game underdog
                if away_price > 0 and away_price < home_price:
                    underdog_team = away["name"]
                    underdog_price = away_price
                elif home_price > 0:
                    underdog_team = home["name"]
                    underdog_price = home_price
                else:
                    continue

                quality = "upcoming" if both_elite else "watch"
                game_time = event.get("date", "")[11:16]
                reason = f"Starts at {game_time} UTC — {away['record']} vs {home['record']}"
                if both_elite:
                    reason = f"ELITE MATCHUP — {reason}"

                opp = SwingOpportunity(
                    away_team=away["name"], home_team=home["name"],
                    away_score=0, home_score=0,
                    period=0, clock=game_time or "Soon",
                    away_record=away["record"], home_record=home["record"],
                    away_wins=away["wins"], home_wins=home["wins"],
                    underdog_ticker="", underdog_team=underdog_team,
                    underdog_price=underdog_price, favorite_price=0,
                    market_volume=vol, score_diff=0,
                    quality=quality, reason=reason,
                )
                opps.append(opp)
                continue

            # --- Live games: find swing opportunities ---

            # Game must not be tied
            if away["score"] == home["score"]:
                continue

            # Determine who's losing (the underdog we want to buy)
            if away["score"] < home["score"]:
                underdog = away
                leader = home
            else:
                underdog = home
                leader = away

            score_diff = leader["score"] - underdog["score"]

            # Don't buy if the game is a blowout (>20 points)
            if score_diff > 20:
                continue

            # Find Kalshi market for the underdog
            underdog_ticker = f"KXNBAGAME-{date_str}{away['abbr']}{home['abbr']}-{underdog['abbr']}"

            try:
                mr = await self._http.get(f"{KALSHI_BASE}/trade-api/v2/markets/{underdog_ticker}")
                if mr.status_code != 200:
                    continue
                market = mr.json().get("market", mr.json())
                underdog_price = float(market.get("yes_ask_dollars", "0") or "0")
                favorite_price = float(market.get("no_bid_dollars", "0") or "0")
                volume = float(market.get("volume_fp", "0") or "0")

                if underdog_price <= 0 or volume < 500:
                    continue
            except Exception:
                continue

            # Score the opportunity
            quality, reason = self._score_opportunity(
                underdog, leader, score_diff, underdog_price, period
            )

            opp = SwingOpportunity(
                away_team=away["name"],
                home_team=home["name"],
                away_score=away["score"],
                home_score=home["score"],
                period=period,
                clock=clock,
                away_record=away["record"],
                home_record=home["record"],
                away_wins=away["wins"],
                home_wins=home["wins"],
                underdog_ticker=underdog_ticker,
                underdog_team=underdog["name"],
                underdog_price=underdog_price,
                favorite_price=favorite_price,
                market_volume=volume,
                score_diff=score_diff,
                quality=quality,
                reason=reason,
            )
            opps.append(opp)

        # Sort: live signals first, then upcoming
        quality_order = {"great": 0, "good": 1, "watch": 2, "upcoming": 3}
        opps.sort(key=lambda o: quality_order.get(o.quality, 4))

        self.opportunities = opps
        return opps

    def _score_opportunity(
        self, underdog: dict, leader: dict, diff: int,
        price: float, period: int
    ) -> Tuple[str, str]:
        """Score a swing opportunity as great/good/watch."""
        both_elite = underdog["wins"] >= 50 and leader["wins"] >= 50
        both_strong = underdog["wins"] >= 45 and leader["wins"] >= 45

        # Great: elite matchup, small deficit, cheap price, early game
        if both_elite and diff <= 8 and price <= 0.35 and period <= 2:
            return "great", f"Elite matchup, {underdog['name']} only down {diff} at ${price:.2f}"

        if both_strong and diff <= 6 and price <= 0.40 and period <= 2:
            return "great", f"Strong teams, {underdog['name']} down {diff} at ${price:.2f}"

        # Good: decent matchup, moderate deficit
        if both_strong and diff <= 10 and price <= 0.35:
            return "good", f"Good teams, {underdog['name']} down {diff} at ${price:.2f}"

        if diff <= 5 and price <= 0.45 and period <= 3:
            return "good", f"Close game, {underdog['name']} down {diff} at ${price:.2f}"

        # Watch: interesting but not actionable yet
        return "watch", f"{underdog['name']} down {diff}, price ${price:.2f}"

    def _get_date_str(self, event: dict) -> str:
        """Extract date string for Kalshi ticker from ESPN event.

        Kalshi uses the game's calendar date in US Eastern time,
        so a game at 1:30am UTC on Apr 3 is actually an Apr 2 game.
        """
        date = event.get("date", "")
        if date:
            from datetime import datetime, timedelta, timezone
            try:
                dt = datetime.fromisoformat(date.replace("Z", "+00:00"))
                # Convert to US Eastern (UTC-4 during EDT)
                eastern = dt - timedelta(hours=4)
                return eastern.strftime("%y").upper() + eastern.strftime("%b%d").upper()
            except Exception:
                pass
        return "26APR02"

    def snapshot(self) -> dict:
        return {
            "opportunities": [o.to_dict() for o in self.opportunities],
            "count": len(self.opportunities),
            "great_count": sum(1 for o in self.opportunities if o.quality == "great"),
            "good_count": sum(1 for o in self.opportunities if o.quality == "good"),
        }
