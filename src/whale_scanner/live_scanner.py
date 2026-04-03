"""
Live Game Scanner — combines ESPN scores + Kalshi prices + whale flow.

The edge: spot games where the live score doesn't match the market price.
E.g., team leading by 10 but still priced at 50% = buy opportunity.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import httpx

KALSHI_BASE = "https://api.elections.kalshi.com"
ESPN_NBA = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_NHL = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard"

# Map ESPN team short names to Kalshi ticker fragments
# Kalshi uses city names; ESPN uses mascot names
ESPN_TO_KALSHI = {
    # NBA
    "Timberwolves": "MIN", "Pistons": "DET", "Suns": "PHX", "Hornets": "CHA",
    "Lakers": "LAL", "Thunder": "OKC", "Cavaliers": "CLE", "Warriors": "GSW",
    "Pelicans": "NOP", "Trail Blazers": "POR", "Spurs": "SA", "Clippers": "LAC",
    "Celtics": "BOS", "Heat": "MIA", "Bucks": "MIL", "Rockets": "HOU",
    "Nets": "BKN", "Knicks": "NYK", "76ers": "PHI", "Raptors": "TOR",
    "Bulls": "CHI", "Pacers": "IND", "Hawks": "ATL", "Magic": "ORL",
    "Wizards": "WAS", "Mavericks": "DAL", "Grizzlies": "MEM", "Kings": "SAC",
    "Nuggets": "DEN", "Jazz": "UTA", "Wolves": "MIN",
    # NHL
    "Sabres": "BUF", "Senators": "OTT", "Penguins": "PIT", "Lightning": "TB",
    "Bruins": "BOS", "Panthers": "FLA", "Canadiens": "MTL", "Rangers": "NYR",
    "Red Wings": "DET", "Flyers": "PHI", "Blue Jackets": "CBJ", "Hurricanes": "CAR",
    "Capitals": "WAS", "Devils": "NJ", "Jets": "WPG", "Stars": "DAL",
    "Canucks": "VAN", "Wild": "MIN", "Blackhawks": "CHI", "Oilers": "EDM",
    "Flames": "CGY", "Golden Knights": "VGK", "Maple Leafs": "TOR", "Sharks": "SJ",
    "Kraken": "SEA", "Kings": "LA", "Predators": "NSH", "Mammoth": "COL",
}


@dataclass
class LiveGame:
    """A live game with score + market data."""
    # ESPN data
    sport: str  # "nba" or "nhl"
    away_team: str
    home_team: str
    away_score: int
    home_score: int
    period: int
    clock: str
    status: str  # "In Progress", "Scheduled", "Final"
    espn_id: str

    # Kalshi data (if matched)
    kalshi_ticker: str = ""
    kalshi_title: str = ""
    away_price: float = 0.0  # price for away team to win
    home_price: float = 0.0  # price for home team to win
    market_volume: float = 0.0

    # Whale flow
    away_whale_flow: float = 0.0
    home_whale_flow: float = 0.0

    # Computed
    score_diff: int = 0  # positive = away leading
    implied_leader_pct: float = 0.0  # market's implied % for the leading team
    alert: str = ""  # mismatch alert text
    alert_level: str = ""  # "hot", "warm", "none"

    def to_dict(self) -> dict:
        return {
            "sport": self.sport,
            "away_team": self.away_team,
            "home_team": self.home_team,
            "away_score": self.away_score,
            "home_score": self.home_score,
            "period": self.period,
            "clock": self.clock,
            "status": self.status,
            "kalshi_ticker": self.kalshi_ticker,
            "away_price": self.away_price,
            "home_price": self.home_price,
            "market_volume": self.market_volume,
            "away_whale_flow": round(self.away_whale_flow, 0),
            "home_whale_flow": round(self.home_whale_flow, 0),
            "score_diff": self.score_diff,
            "implied_leader_pct": round(self.implied_leader_pct, 1),
            "alert": self.alert,
            "alert_level": self.alert_level,
        }


class LiveScanner:
    """Scans ESPN for scores and matches with Kalshi markets."""

    def __init__(self):
        self._http: Optional[httpx.AsyncClient] = None
        self.games: List[LiveGame] = []
        self._kalshi_cache: Dict[str, dict] = {}  # ticker -> market data
        self._last_scan: float = 0

    async def start(self):
        self._http = httpx.AsyncClient(timeout=15.0)

    async def stop(self):
        if self._http:
            await self._http.aclose()

    async def scan(self) -> List[LiveGame]:
        """Full scan: ESPN scores → Kalshi match → whale flow → alerts."""
        games = []

        # Fetch scores from ESPN
        nba_games = await self._fetch_espn(ESPN_NBA, "nba")
        nhl_games = await self._fetch_espn(ESPN_NHL, "nhl")
        games.extend(nba_games)
        games.extend(nhl_games)

        # Match each game to Kalshi markets and enrich
        for game in games:
            await self._match_kalshi(game)
            await self._fetch_whale_flow(game)
            self._compute_alert(game)

        # Sort: live games first, then by alert level
        alert_order = {"hot": 0, "warm": 1, "none": 2}
        status_order = {"In Progress": 0, "End of Period": 0, "Halftime": 0, "Scheduled": 1, "Final": 2}
        games.sort(key=lambda g: (
            status_order.get(g.status, 1),
            alert_order.get(g.alert_level, 2),
        ))

        self.games = games
        self._last_scan = time.time()
        return games

    async def _fetch_espn(self, url: str, sport: str) -> List[LiveGame]:
        """Fetch live scores from ESPN API."""
        games = []
        try:
            resp = await self._http.get(url)
            if resp.status_code != 200:
                return games
            data = resp.json()

            for event in data.get("events", []):
                status_type = event["status"]["type"]["description"]
                period = event["status"].get("period", 0)
                clock = event["status"].get("displayClock", "")
                espn_id = event.get("id", "")

                comp = event.get("competitions", [{}])[0]
                teams = {}
                for t in comp.get("competitors", []):
                    ha = t.get("homeAway", "")
                    teams[ha] = {
                        "name": t["team"]["shortDisplayName"],
                        "score": int(t.get("score", "0") or "0"),
                    }

                away = teams.get("away", {"name": "?", "score": 0})
                home = teams.get("home", {"name": "?", "score": 0})

                games.append(LiveGame(
                    sport=sport,
                    away_team=away["name"],
                    home_team=home["name"],
                    away_score=away["score"],
                    home_score=home["score"],
                    period=period,
                    clock=clock,
                    status=status_type,
                    espn_id=espn_id,
                ))
        except Exception as e:
            print(f"[scanner] ESPN {sport} error: {e}")
        return games

    async def _match_kalshi(self, game: LiveGame):
        """Try to find a Kalshi winner market for this game."""
        away_abbr = ESPN_TO_KALSHI.get(game.away_team, "")
        home_abbr = ESPN_TO_KALSHI.get(game.home_team, "")

        if not away_abbr or not home_abbr:
            return

        # Build possible Kalshi ticker patterns
        prefix = "KXNBAGAME" if game.sport == "nba" else "KXNHLGAME"
        date_str = "26APR02"  # TODO: dynamic date

        # Kalshi format: KXNBAGAME-26APR02MINDET-MIN (away+home, then side)
        base = f"{prefix}-{date_str}{away_abbr}{home_abbr}"
        away_ticker = f"{base}-{away_abbr}"
        home_ticker = f"{base}-{home_abbr}"

        # Try to fetch both sides
        for ticker, is_away in [(away_ticker, True), (home_ticker, False)]:
            try:
                resp = await self._http.get(f"{KALSHI_BASE}/trade-api/v2/markets/{ticker}")
                if resp.status_code == 200:
                    m = resp.json().get("market", resp.json())
                    yes_bid = float(m.get("yes_bid_dollars", "0") or "0")
                    vol = float(m.get("volume_fp", "0") or "0")

                    if is_away:
                        game.away_price = yes_bid
                        game.home_price = round(1 - yes_bid, 2)
                        game.kalshi_ticker = ticker
                    else:
                        game.home_price = yes_bid
                        game.away_price = round(1 - yes_bid, 2)
                        if not game.kalshi_ticker:
                            game.kalshi_ticker = ticker

                    game.market_volume = max(game.market_volume, vol)
                    game.kalshi_title = m.get("title", "")
            except Exception:
                pass

    async def _fetch_whale_flow(self, game: LiveGame):
        """Get recent whale flow for this game's market."""
        if not game.kalshi_ticker:
            return

        # Get the base event ticker (without the side suffix)
        parts = game.kalshi_ticker.rsplit("-", 1)
        if len(parts) < 2:
            return

        base_ticker = parts[0]
        away_abbr = ESPN_TO_KALSHI.get(game.away_team, "")
        home_abbr = ESPN_TO_KALSHI.get(game.home_team, "")

        for suffix, is_away in [(away_abbr, True), (home_abbr, False)]:
            ticker = f"{base_ticker}-{suffix}"
            try:
                resp = await self._http.get(
                    f"{KALSHI_BASE}/trade-api/v2/markets/trades",
                    params={"ticker": ticker, "limit": 100},
                )
                if resp.status_code != 200:
                    continue
                trades = resp.json().get("trades", [])

                for t in trades:
                    count = float(t.get("count_fp", "0") or "0")
                    side = t.get("taker_side", "yes")
                    price = float(t.get(f"{side}_price_dollars", "0") or "0")
                    amt = count * price

                    if amt >= 50:
                        if (is_away and side == "yes") or (not is_away and side == "no"):
                            game.away_whale_flow += amt
                        else:
                            game.home_whale_flow += amt
            except Exception:
                pass

    def _compute_alert(self, game: LiveGame):
        """Detect score/price mismatches."""
        if game.status not in ("In Progress", "End of Period", "Halftime"):
            game.alert_level = "none"
            return

        if game.away_price <= 0 or game.home_price <= 0:
            game.alert_level = "none"
            return

        game.score_diff = game.away_score - game.home_score

        # Who's leading?
        if game.away_score > game.home_score:
            leader = game.away_team
            leader_price = game.away_price
            trail_price = game.home_price
            diff = game.away_score - game.home_score
        elif game.home_score > game.away_score:
            leader = game.home_team
            leader_price = game.home_price
            trail_price = game.away_price
            diff = game.home_score - game.away_score
        else:
            game.alert_level = "none"
            game.alert = "Tied"
            game.implied_leader_pct = 50
            return

        game.implied_leader_pct = leader_price * 100

        # Alert logic: leader by significant margin but price doesn't reflect it
        if game.sport == "nba":
            # NBA: 10+ point lead but < 65% implied = mispriced
            if diff >= 10 and leader_price < 0.65:
                game.alert = f"{leader} up {diff} but only {leader_price:.0%}"
                game.alert_level = "hot"
            elif diff >= 6 and leader_price < 0.55:
                game.alert = f"{leader} up {diff} but only {leader_price:.0%}"
                game.alert_level = "warm"
            elif diff >= 3 and leader_price < 0.40:
                game.alert = f"{leader} up {diff} but only {leader_price:.0%}"
                game.alert_level = "warm"
            else:
                game.alert_level = "none"
        elif game.sport == "nhl":
            # NHL: 2+ goal lead but < 70% = mispriced
            if diff >= 2 and leader_price < 0.70:
                game.alert = f"{leader} up {diff} goals but only {leader_price:.0%}"
                game.alert_level = "hot"
            elif diff >= 1 and leader_price < 0.50:
                game.alert = f"{leader} up {diff} goal but only {leader_price:.0%}"
                game.alert_level = "warm"
            else:
                game.alert_level = "none"

    def snapshot(self) -> dict:
        return {
            "games": [g.to_dict() for g in self.games],
            "live_count": sum(1 for g in self.games if g.status in ("In Progress", "End of Period", "Halftime")),
            "alerts": sum(1 for g in self.games if g.alert_level in ("hot", "warm")),
            "updated_at": self._last_scan,
        }
