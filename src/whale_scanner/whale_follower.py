"""
Whale Follower Strategy — follow smart money on Kalshi.

Monitors real-time trade flow, detects whale consensus,
and places small trades ($5-10) following the dominant side.

Supports paper-trade mode (default) and live mode.
"""

import asyncio
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx

KALSHI_BASE = "https://api.elections.kalshi.com"

# --- Configuration ---

@dataclass
class WhaleConfig:
    # Whale detection — tuned for sports markets (91% historical win rate)
    min_whale_dollars: float = 50         # Min trade size to count as whale
    min_whale_ratio: float = 3.0          # Money ratio (dominant / minority)
    min_whale_volume: float = 200         # Min total whale $ on dominant side
    min_distinct_traders: int = 3         # Multiple independent whales must agree

    # Position sizing — conservative for $100 budget
    trade_size_dollars: float = 5.0       # $5 per trade
    max_trade_size: float = 10.0          # Hard cap per trade
    max_open_positions: int = 8           # More positions = more diversification
    max_total_deployed: float = 50.0      # Never risk more than half the budget
    budget: float = 100.0                 # Total budget

    # Market filters — SPORTS ONLY (where whales have real edge)
    series_whitelist: List[str] = field(default_factory=lambda: [
        # NBA
        "KXNBAGAME", "KXNBASPREAD", "KXNBA1HTOTAL", "KXNBA1HWINNER",
        "KXNBATOTAL", "KXNBAAST", "KXNBAREB", "KXNBAPTS",
        # NHL
        "KXNHLGAME", "KXNHLTOTAL", "KXNHLSPREAD", "KXNHLGOAL",
        # MLB
        "KXMLBGAME", "KXMLBTOTAL", "KXMLBSPREAD",
        # Tennis
        "KXATPMATCH", "KXATPCHALLENGERMATCH", "KXATPSETWINNER",
        "KXWTAMATCH",
        # Soccer
        "KXBRASILEIROGAME", "KXDIMAYORGAME", "KXBALLERLEAGUEGAME",
        # Cricket
        "KXT20MATCH",
    ])
    min_market_volume: float = 500        # Decent liquidity required

    # Timing
    scan_interval_seconds: int = 20       # Faster scans to catch signals
    max_minutes_before_close: float = 1440  # Up to 24h out
    min_minutes_before_close: float = 2   # Don't chase last-second

    # Mode
    paper_mode: bool = True


@dataclass
class WhaleSignal:
    """A detected whale consensus signal."""
    ticker: str
    title: str
    side: str  # "yes" or "no"
    whale_volume_dominant: float
    whale_volume_minority: float
    whale_ratio: float
    whale_trade_count: int
    market_yes_bid: float
    market_no_bid: float
    minutes_to_close: float
    detected_at: str
    confidence: float  # 0-1 based on ratio + volume
    yes_sub_title: str = ""
    no_sub_title: str = ""

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "title": self.title,
            "side": self.side,
            "whale_volume": self.whale_volume_dominant,
            "whale_ratio": round(self.whale_ratio, 1),
            "whale_trades": self.whale_trade_count,
            "confidence": round(self.confidence, 2),
            "minutes_to_close": round(self.minutes_to_close, 1),
            "detected_at": self.detected_at,
            "yes_sub_title": self.yes_sub_title,
            "no_sub_title": self.no_sub_title,
        }


@dataclass
class Position:
    """A trade position (paper or live)."""
    ticker: str
    title: str
    side: str
    contracts: int
    entry_price: float  # dollars per contract
    cost: float  # total cost
    opened_at: str
    signal: dict
    yes_sub_title: str = ""  # What Kalshi shows for YES side
    no_sub_title: str = ""   # What Kalshi shows for NO side
    status: str = "open"  # open, won, lost, expired
    pnl: float = 0.0
    closed_at: str = ""


class WhaleFollower:
    """
    Scans Kalshi markets for whale consensus and places follow trades.
    """

    def __init__(self, config: Optional[WhaleConfig] = None):
        self.config = config or WhaleConfig()
        self._http: Optional[httpx.AsyncClient] = None
        self._seen_signals: set = set()  # ticker+side combos already traded
        self.positions: List[Position] = []
        self.closed_positions: List[Position] = []
        self.signals_log: List[WhaleSignal] = []
        self._trade_log_path = Path(__file__).parent / "trade_log.json"

    async def start(self):
        self._http = httpx.AsyncClient(timeout=20.0)
        self._load_log()
        mode = "PAPER" if self.config.paper_mode else "LIVE"
        print(f"\n{'='*60}")
        print(f"  🐳 WHALE FOLLOWER — {mode} MODE")
        print(f"  Budget: ${self.config.budget:.0f} | Trade size: ${self.config.trade_size_dollars:.0f}")
        print(f"  Max positions: {self.config.max_open_positions}")
        print(f"{'='*60}\n")

    async def stop(self):
        self._save_log()
        if self._http:
            await self._http.aclose()

    # ------------------------------------------------------------------
    # Core loop
    # ------------------------------------------------------------------

    async def run(self):
        """Main loop: scan → detect → trade → repeat."""
        await self.start()
        try:
            while True:
                signals = await self.scan_for_signals()
                for signal in signals:
                    await self.maybe_trade(signal)
                await self._check_settled_positions()
                await asyncio.sleep(self.config.scan_interval_seconds)
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping whale follower...")
            self._print_summary()
        finally:
            await self.stop()

    # ------------------------------------------------------------------
    # Signal detection
    # ------------------------------------------------------------------

    async def scan_for_signals(self) -> List[WhaleSignal]:
        """Scan active markets for whale consensus using two passes."""
        signals = []

        # --- Pass 1: Global trade stream (catches active flow) ---
        sports_tickers: set = set()
        by_ticker: Dict[str, list] = {}
        try:
            resp = await self._http.get(
                f"{KALSHI_BASE}/trade-api/v2/markets/trades",
                params={"limit": 200},
            )
            if resp.status_code == 200:
                for t in resp.json().get("trades", []):
                    tk = t.get("ticker", "")
                    if any(tk.startswith(s) for s in self.config.series_whitelist):
                        by_ticker.setdefault(tk, []).append(t)
                        sports_tickers.add(tk)
        except Exception as e:
            print(f"  [scan] error: {e}")

        # --- Pass 2: Deep-fetch trades for each sports ticker ---
        # This gets more history per market (not just what's in the global stream)
        for ticker in list(sports_tickers):
            if f"{ticker}:" in str(self._seen_signals):
                continue  # Already traded this one
            try:
                tresp = await self._http.get(
                    f"{KALSHI_BASE}/trade-api/v2/markets/trades",
                    params={"ticker": ticker, "limit": 100},
                )
                if tresp.status_code == 200:
                    deep_trades = tresp.json().get("trades", [])
                    # Merge with existing (deduplicate by trade_id)
                    existing_ids = {t.get("trade_id") for t in by_ticker.get(ticker, [])}
                    for dt in deep_trades:
                        if dt.get("trade_id") not in existing_ids:
                            by_ticker.setdefault(ticker, []).append(dt)
            except Exception:
                pass

        # --- Analyze each ticker for whale consensus ---
        for ticker, ticker_trades in by_ticker.items():
            signal = await self._analyze_ticker(ticker, ticker_trades)
            if signal:
                signals.append(signal)

        if signals:
            print(f"\n  📡 Found {len(signals)} whale signal(s):")
            for s in signals:
                print(f"     {s.side.upper():3s} {s.ticker[:40]} | "
                      f"${s.whale_volume_dominant:,.0f} from {s.whale_trade_count} traders ({s.whale_ratio:.0f}x) | "
                      f"conf: {s.confidence:.0%}")

        return signals

    async def _analyze_ticker(self, ticker: str, trades: list) -> Optional[WhaleSignal]:
        """Analyze a ticker's trades for whale consensus.

        Clusters trades by timing to estimate distinct traders,
        then requires multiple independent whales agreeing on the same side.
        """
        # Cluster trades by timing (trades within 3s of each other on same
        # side are likely the same person splitting an order)
        sorted_trades = sorted(trades, key=lambda t: t.get("created_time", ""))

        yes_traders = 0  # distinct trader clusters on YES
        no_traders = 0
        yes_whale = 0.0
        no_whale = 0.0

        prev_time = None
        prev_side = None
        cluster_amt = 0.0

        for t in sorted_trades:
            count = float(t.get("count_fp", "0") or "0")
            side = t.get("taker_side", "yes")
            price = float(t.get(f"{side}_price_dollars", "0") or "0")
            amt = count * price

            if amt < self.config.min_whale_dollars:
                continue

            cur_time = t.get("created_time", "")

            # Check if this is a new trader (different side or >3s gap)
            is_new_trader = True
            if prev_time and prev_side == side:
                try:
                    from datetime import datetime as _dt
                    t1 = _dt.fromisoformat(prev_time.replace("Z", "+00:00"))
                    t2 = _dt.fromisoformat(cur_time.replace("Z", "+00:00"))
                    gap = abs((t2 - t1).total_seconds())
                    if gap <= 3:
                        is_new_trader = False
                except Exception:
                    pass

            if is_new_trader and cluster_amt > 0:
                # Close previous cluster
                if prev_side == "yes":
                    yes_traders += 1
                    yes_whale += cluster_amt
                else:
                    no_traders += 1
                    no_whale += cluster_amt
                cluster_amt = 0.0

            cluster_amt += amt
            prev_time = cur_time
            prev_side = side

        # Close final cluster
        if cluster_amt > 0 and prev_side:
            if prev_side == "yes":
                yes_traders += 1
                yes_whale += cluster_amt
            else:
                no_traders += 1
                no_whale += cluster_amt

        # Determine dominant side
        if yes_whale > no_whale:
            dominant, minority = "yes", "no"
            dom_vol, min_vol = yes_whale, no_whale
            dom_count = yes_traders
            min_count = no_traders
        else:
            dominant, minority = "no", "yes"
            dom_vol, min_vol = no_whale, yes_whale
            dom_count = no_traders
            min_count = yes_traders

        # Check thresholds
        if dom_vol < self.config.min_whale_volume:
            return None
        if dom_count < self.config.min_distinct_traders:
            return None

        ratio = dom_vol / max(min_vol, 1)
        if ratio < self.config.min_whale_ratio:
            return None

        # Skip if we already have a position on this ticker+side
        sig_key = f"{ticker}:{dominant}"
        if sig_key in self._seen_signals:
            return None

        # Get market info
        try:
            mresp = await self._http.get(f"{KALSHI_BASE}/trade-api/v2/markets/{ticker}")
            if mresp.status_code != 200:
                return None
            market = mresp.json().get("market", mresp.json())
        except Exception:
            return None

        # Check market volume
        vol = float(market.get("volume_fp", "0") or "0")
        if vol < self.config.min_market_volume:
            return None

        # Check time to resolution — prefer expected_expiration_time (actual game end)
        # over close_time (which can be weeks out as a fallback)
        time_str = (
            market.get("expected_expiration_time")
            or market.get("close_time", "")
        )
        if not time_str:
            return None
        resolve_dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        mins_left = (resolve_dt - now).total_seconds() / 60

        if mins_left < self.config.min_minutes_before_close:
            return None
        if mins_left > self.config.max_minutes_before_close:
            return None

        # Confidence: traders agreeing (40%) + money ratio (30%) + volume (30%)
        trader_score = min(1.0, dom_count / 10)  # 10+ traders = max
        ratio_score = min(1.0, ratio / 15)       # 15x+ ratio = max
        vol_score = min(1.0, dom_vol / 3000)     # $3k+ = max
        conf = trader_score * 0.4 + ratio_score * 0.3 + vol_score * 0.3

        yes_bid = float(market.get("yes_bid_dollars", "0") or "0")
        no_bid = float(market.get("no_bid_dollars", "0") or "0")

        return WhaleSignal(
            ticker=ticker,
            title=market.get("title", ticker),
            side=dominant,
            whale_volume_dominant=dom_vol,
            whale_volume_minority=min_vol,
            whale_ratio=ratio,
            whale_trade_count=dom_count,
            market_yes_bid=yes_bid,
            market_no_bid=no_bid,
            minutes_to_close=mins_left,
            detected_at=now.isoformat(),
            confidence=conf,
            yes_sub_title=market.get("yes_sub_title", ""),
            no_sub_title=market.get("no_sub_title", ""),
        )

    # ------------------------------------------------------------------
    # Trade execution
    # ------------------------------------------------------------------

    async def maybe_trade(self, signal: WhaleSignal):
        """Decide whether to trade on a signal and execute."""
        # Check position limits
        open_positions = [p for p in self.positions if p.status == "open"]
        if len(open_positions) >= self.config.max_open_positions:
            print(f"  ⏸️  Skip {signal.ticker}: max positions reached")
            return

        total_deployed = sum(p.cost for p in open_positions)
        if total_deployed >= self.config.max_total_deployed:
            print(f"  ⏸️  Skip {signal.ticker}: max capital deployed")
            return

        # Calculate position size
        entry_price = signal.market_yes_bid if signal.side == "yes" else signal.market_no_bid
        if entry_price <= 0 or entry_price >= 1.0:
            return

        trade_dollars = min(self.config.trade_size_dollars, self.config.max_trade_size)
        contracts = max(1, int(trade_dollars / entry_price))
        cost = contracts * entry_price

        if cost > self.config.max_trade_size:
            contracts = max(1, int(self.config.max_trade_size / entry_price))
            cost = contracts * entry_price

        # Mark signal as seen
        sig_key = f"{signal.ticker}:{signal.side}"
        self._seen_signals.add(sig_key)
        self.signals_log.append(signal)

        if self.config.paper_mode:
            await self._paper_trade(signal, contracts, entry_price, cost)
        else:
            await self._live_trade(signal, contracts, entry_price, cost)

    async def _paper_trade(self, signal: WhaleSignal, contracts: int, entry_price: float, cost: float):
        """Log a paper trade."""
        pos = Position(
            ticker=signal.ticker,
            title=signal.title,
            side=signal.side,
            contracts=contracts,
            entry_price=entry_price,
            cost=round(cost, 2),
            opened_at=datetime.now(timezone.utc).isoformat(),
            signal=signal.to_dict(),
            yes_sub_title=signal.yes_sub_title,
            no_sub_title=signal.no_sub_title,
        )
        self.positions.append(pos)
        self._save_log()

        print(f"\n  📝 PAPER TRADE:")
        print(f"     {signal.side.upper()} {signal.ticker}")
        print(f"     {contracts} contracts @ ${entry_price:.2f} = ${cost:.2f}")
        print(f"     Whale signal: ${signal.whale_volume_dominant:,.0f} ({signal.whale_ratio:.0f}x ratio)")
        print(f"     Market: {signal.title}")

    async def _live_trade(self, signal: WhaleSignal, contracts: int, entry_price: float, cost: float):
        """Execute a live trade via Kalshi API."""
        try:
            from src.clients.kalshi_client import KalshiClient

            async with KalshiClient() as client:
                price_cents = int(entry_price * 100)
                order = await client.place_order(
                    ticker=signal.ticker,
                    client_order_id=str(uuid.uuid4()),
                    side=signal.side,
                    action="buy",
                    count=contracts,
                    type_="limit",
                    yes_price=price_cents if signal.side == "yes" else None,
                    no_price=price_cents if signal.side == "no" else None,
                )

                pos = Position(
                    ticker=signal.ticker,
                    title=signal.title,
                    side=signal.side,
                    contracts=contracts,
                    entry_price=entry_price,
                    cost=round(cost, 2),
                    opened_at=datetime.now(timezone.utc).isoformat(),
                    signal=signal.to_dict(),
                )
                self.positions.append(pos)
                self._save_log()

                print(f"\n  🔥 LIVE TRADE PLACED:")
                print(f"     {signal.side.upper()} {signal.ticker}")
                print(f"     {contracts} contracts @ ${entry_price:.2f} = ${cost:.2f}")
                print(f"     Order: {order}")

        except Exception as e:
            print(f"  ❌ Trade failed: {e}")

    # ------------------------------------------------------------------
    # Position settlement
    # ------------------------------------------------------------------

    async def _check_settled_positions(self):
        """Check if any open positions have settled."""
        for pos in self.positions:
            if pos.status != "open":
                continue
            try:
                resp = await self._http.get(f"{KALSHI_BASE}/trade-api/v2/markets/{pos.ticker}")
                if resp.status_code != 200:
                    continue
                market = resp.json().get("market", resp.json())
                result = market.get("result", "")
                status = market.get("status", "")

                if status in ("settled", "finalized", "closed") and result in ("yes", "no"):
                    won = result == pos.side
                    if won:
                        pos.pnl = round(pos.contracts * (1.0 - pos.entry_price), 2)
                        pos.status = "won"
                    else:
                        pos.pnl = round(-pos.cost, 2)
                        pos.status = "lost"
                    pos.closed_at = datetime.now(timezone.utc).isoformat()
                    self.closed_positions.append(pos)
                    self._save_log()

                    icon = "✅" if won else "❌"
                    print(f"\n  {icon} SETTLED: {pos.ticker}")
                    print(f"     Side: {pos.side.upper()} | Result: {result.upper()} | P&L: ${pos.pnl:+.2f}")

            except Exception:
                continue

    # ------------------------------------------------------------------
    # Logging & state
    # ------------------------------------------------------------------

    def _save_log(self):
        data = {
            "positions": [
                {
                    "ticker": p.ticker, "title": p.title, "side": p.side,
                    "contracts": p.contracts, "entry_price": p.entry_price,
                    "cost": p.cost, "opened_at": p.opened_at, "status": p.status,
                    "pnl": p.pnl, "closed_at": p.closed_at, "signal": p.signal,
                }
                for p in self.positions
            ],
            "closed": [
                {
                    "ticker": p.ticker, "title": p.title, "side": p.side,
                    "contracts": p.contracts, "entry_price": p.entry_price,
                    "cost": p.cost, "pnl": p.pnl, "status": p.status,
                    "opened_at": p.opened_at, "closed_at": p.closed_at,
                }
                for p in self.closed_positions
            ],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._trade_log_path.write_text(json.dumps(data, indent=2))

    def _load_log(self):
        if self._trade_log_path.exists():
            try:
                data = json.loads(self._trade_log_path.read_text())
                # Restore closed positions for stats
                for p in data.get("closed", []):
                    self.closed_positions.append(Position(
                        ticker=p["ticker"], title=p.get("title", ""), side=p["side"],
                        contracts=p["contracts"], entry_price=p["entry_price"],
                        cost=p["cost"], opened_at=p["opened_at"], signal={},
                        status=p["status"], pnl=p["pnl"], closed_at=p.get("closed_at", ""),
                    ))
            except Exception:
                pass

    def _print_summary(self):
        total_pnl = sum(p.pnl for p in self.closed_positions)
        wins = sum(1 for p in self.closed_positions if p.status == "won")
        losses = sum(1 for p in self.closed_positions if p.status == "lost")
        total = wins + losses
        open_count = sum(1 for p in self.positions if p.status == "open")
        open_cost = sum(p.cost for p in self.positions if p.status == "open")

        print(f"\n{'='*60}")
        print(f"  🐳 WHALE FOLLOWER SUMMARY")
        print(f"{'='*60}")
        print(f"  Settled: {total} trades ({wins}W / {losses}L)")
        if total > 0:
            print(f"  Win rate: {wins/total*100:.0f}%")
        print(f"  Total P&L: ${total_pnl:+.2f}")
        print(f"  Open positions: {open_count} (${open_cost:.2f} deployed)")
        print(f"  Signals detected: {len(self.signals_log)}")
        print(f"{'='*60}\n")

    def get_status(self) -> dict:
        """Return current status for the frontend API."""
        total_pnl = sum(p.pnl for p in self.closed_positions)
        wins = sum(1 for p in self.closed_positions if p.status == "won")
        losses = sum(1 for p in self.closed_positions if p.status == "lost")
        open_positions = [p for p in self.positions if p.status == "open"]

        return {
            "mode": "paper" if self.config.paper_mode else "live",
            "total_pnl": round(total_pnl, 2),
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / max(wins + losses, 1) * 100, 1),
            "open_positions": [
                {"ticker": p.ticker, "title": p.title, "side": p.side,
                 "cost": p.cost, "opened_at": p.opened_at,
                 "yes_sub_title": p.yes_sub_title, "no_sub_title": p.no_sub_title}
                for p in open_positions
            ],
            "recent_closed": [
                {"ticker": p.ticker, "title": p.title, "side": p.side,
                 "pnl": p.pnl, "status": p.status,
                 "yes_sub_title": p.yes_sub_title, "no_sub_title": p.no_sub_title}
                for p in self.closed_positions[-10:]
            ],
            "signals_count": len(self.signals_log),
            "budget_remaining": round(
                self.config.budget - sum(p.cost for p in open_positions), 2
            ),
        }


async def main():
    """Run the whale follower in paper mode."""
    config = WhaleConfig(
        paper_mode=True,
        trade_size_dollars=5.0,
        budget=100.0,
    )
    follower = WhaleFollower(config)
    await follower.run()


if __name__ == "__main__":
    asyncio.run(main())
