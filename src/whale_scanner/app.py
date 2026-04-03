"""
Kalshi Whale Scanner + Follower — FastAPI backend.

Combines whale trade detection with a follow-the-whales paper/live trader.
Serves a retro terminal dashboard showing whale flow, signals, and P&L.
"""

import asyncio
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from src.whale_scanner.whale_follower import WhaleConfig, WhaleFollower
from src.whale_scanner.live_scanner import LiveScanner
from src.whale_scanner.auto_manager import AutoManager
from src.whale_scanner.swing_scanner import SwingScanner
from src.whale_scanner.swing_trader import SwingTrader, GameWatch

KALSHI_BASE = "https://api.elections.kalshi.com"
TRADES_ENDPOINT = "/trade-api/v2/markets/trades"
MARKET_ENDPOINT = "/trade-api/v2/markets"

WHALE_THRESHOLD_DOLLARS = 100
MAX_WHALES = 25
POLL_INTERVAL = 15


@dataclass
class WhaleTrade:
    ticker: str
    title: str
    side: str
    amount_dollars: float
    count: float
    price_dollars: float
    created_time: str
    trade_id: str

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "title": self.title,
            "side": self.side,
            "amount_dollars": round(self.amount_dollars, 2),
            "count": self.count,
            "price_dollars": self.price_dollars,
            "created_time": self.created_time,
            "trade_id": self.trade_id,
        }


class WhaleStore:
    """In-memory store for recent whale trades."""

    def __init__(self):
        self.whales: List[WhaleTrade] = []
        self.market_cache: Dict[str, str] = {}
        self._seen_ids: set = set()
        self._http: Optional[httpx.AsyncClient] = None

    async def start(self):
        self._http = httpx.AsyncClient(timeout=20.0)

    async def stop(self):
        if self._http:
            await self._http.aclose()

    async def _resolve_title(self, ticker: str) -> str:
        if ticker in self.market_cache:
            return self.market_cache[ticker]
        try:
            resp = await self._http.get(f"{KALSHI_BASE}{MARKET_ENDPOINT}/{ticker}")
            if resp.status_code == 200:
                data = resp.json()
                market = data.get("market", data)
                title = market.get("title", ticker)
                self.market_cache[ticker] = title
                return title
        except Exception:
            pass
        return ticker

    async def poll(self):
        try:
            resp = await self._http.get(
                f"{KALSHI_BASE}{TRADES_ENDPOINT}", params={"limit": 200}
            )
            if resp.status_code != 200:
                return
            trades = resp.json().get("trades", [])

            new_whales = []
            for t in trades:
                tid = t.get("trade_id", "")
                if tid in self._seen_ids:
                    continue
                self._seen_ids.add(tid)

                count = float(t.get("count_fp", "0") or "0")
                yes_price = float(t.get("yes_price_dollars", "0") or "0")
                no_price = float(t.get("no_price_dollars", "0") or "0")
                taker_side = t.get("taker_side", "yes")
                price = yes_price if taker_side == "yes" else no_price
                amount_dollars = count * price

                if amount_dollars >= WHALE_THRESHOLD_DOLLARS:
                    ticker = t.get("ticker", "")
                    title = await self._resolve_title(ticker)
                    new_whales.append(WhaleTrade(
                        ticker=ticker, title=title, side=taker_side,
                        amount_dollars=amount_dollars, count=count,
                        price_dollars=price,
                        created_time=t.get("created_time", ""),
                        trade_id=tid,
                    ))

            if new_whales:
                self.whales = (new_whales + self.whales)[:MAX_WHALES]
            if len(self._seen_ids) > 50_000:
                self._seen_ids = set()
        except Exception as e:
            print(f"[whale-scanner] poll error: {e}")

    def snapshot(self) -> dict:
        total_volume = sum(w.amount_dollars for w in self.whales)
        return {
            "whales": [w.to_dict() for w in self.whales],
            "total_volume_dollars": round(total_volume, 2),
            "count": len(self.whales),
            "updated_at": time.time(),
        }


# --- Global instances ---
store = WhaleStore()
scanner = LiveScanner()
swing = SwingScanner()
auto_mgr = AutoManager(
    api_key=os.environ.get("KALSHI_API_KEY", ""),
    private_key_path=os.environ.get("KALSHI_PRIVATE_KEY_PATH", "kalshi_private_key.pem"),
)
swing_trader = SwingTrader(
    api_key=os.environ.get("KALSHI_API_KEY", ""),
    private_key_path=os.environ.get("KALSHI_PRIVATE_KEY_PATH", "kalshi_private_key.pem"),
)

follower = WhaleFollower(WhaleConfig(
    paper_mode=True,
    trade_size_dollars=5.0,
    budget=100.0,
    scan_interval_seconds=20,
))


async def _poll_loop():
    while True:
        await store.poll()
        await asyncio.sleep(POLL_INTERVAL)


async def _auto_manager_loop():
    """Background task that checks positions every 15s for auto-sell triggers."""
    await auto_mgr.start()
    while True:
        try:
            await auto_mgr.check_positions()
        except Exception as e:
            print(f"[auto-manager] error: {e}")
        await asyncio.sleep(15)


async def _scanner_loop():
    """Background task that scans live games every 15s."""
    await scanner.start()
    await swing.start()
    await swing_trader.start()

    # Set up LAL @ OKC swing trade
    swing_trader.watch_game(GameWatch(
        away_abbr="LAL", home_abbr="OKC",
        away_team="Lakers", home_team="Thunder",
        date_str="26APR02",
        max_budget=10.0,
        max_entry_price=0.28,  # Lakers pre-game at ~$0.24
        profit_target_pct=0.40,  # sell at +40% (~$0.34)
        max_swings=2,
    ))

    while True:
        try:
            await scanner.scan()
            await swing.scan()
            await swing_trader.check()
        except Exception as e:
            print(f"[scanner] error: {e}")
        await asyncio.sleep(15)


async def _follower_loop():
    """Background task that runs the whale follower strategy."""
    await follower.start()
    while True:
        try:
            signals = await follower.scan_for_signals()
            for signal in signals:
                await follower.maybe_trade(signal)
            await follower._check_settled_positions()
        except Exception as e:
            print(f"[whale-follower] error: {e}")
        await asyncio.sleep(follower.config.scan_interval_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await store.start()
    poll_task = asyncio.create_task(_poll_loop())
    scanner_task = asyncio.create_task(_scanner_loop())
    auto_task = asyncio.create_task(_auto_manager_loop())
    follower_task = asyncio.create_task(_follower_loop())
    yield
    poll_task.cancel()
    scanner_task.cancel()
    auto_task.cancel()
    follower_task.cancel()
    await follower.stop()
    await auto_mgr.stop()
    await swing_trader.stop()
    await swing.stop()
    await scanner.stop()
    await store.stop()


app = FastAPI(title="Kalshi Whale Scanner", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = Path(__file__).parent / "frontend" / "index.html"
    return HTMLResponse(html_path.read_text())


@app.get("/api/games")
async def get_games():
    return JSONResponse(scanner.snapshot())


@app.get("/api/swings")
async def get_swings():
    return JSONResponse(swing.snapshot())


@app.get("/api/swing-trader")
async def get_swing_trader():
    return JSONResponse(swing_trader.get_status())


@app.get("/api/auto")
async def get_auto_status():
    return JSONResponse(auto_mgr.get_status())


@app.get("/api/history")
async def get_history():
    """Fetch trade history — fills grouped by market with P&L."""
    try:
        from src.clients.kalshi_client import KalshiClient
        from collections import defaultdict
        client = KalshiClient(
            api_key="5ed556bd-2c97-46f4-bf57-e3e8891ffdb4",
            private_key_path="kalshi_private_key.pem",
        )
        fills_data = await client._make_authenticated_request(
            "GET", "/trade-api/v2/portfolio/fills", params={"limit": 100}
        )
        settle_data = await client._make_authenticated_request(
            "GET", "/trade-api/v2/portfolio/settlements", params={"limit": 100}
        )
        await client.close()

        fills = fills_data.get("fills", [])
        settlements = {s["ticker"]: s for s in settle_data.get("settlements", [])}

        by_market = defaultdict(list)
        for f in fills:
            by_market[f["market_ticker"]].append(f)

        trades = []
        async with httpx.AsyncClient(timeout=10.0) as http:
            for ticker, market_fills in by_market.items():
                mr = await http.get(f"{KALSHI_BASE}{MARKET_ENDPOINT}/{ticker}")
                title = ticker
                result = ""
                sub = ""
                if mr.status_code == 200:
                    md = mr.json().get("market", mr.json())
                    title = md.get("title", ticker)
                    sub = md.get("yes_sub_title", "")
                    result = md.get("result", "")

                buys = [f for f in market_fills if f["action"] == "buy"]
                sells = [f for f in market_fills if f["action"] == "sell"]

                total_contracts = sum(float(f.get("count_fp", "0")) for f in buys)
                buy_cost = sum(
                    float(f.get("yes_price_dollars") or f.get("no_price_dollars") or "0")
                    * float(f.get("count_fp", "0"))
                    for f in buys
                )
                buy_fees = sum(float(f.get("fee_cost", "0") or "0") for f in buys)
                sell_revenue = sum(
                    float(f.get("yes_price_dollars") or f.get("no_price_dollars") or "0")
                    * float(f.get("count_fp", "0"))
                    for f in sells
                )
                sell_fees = sum(float(f.get("fee_cost", "0") or "0") for f in sells)

                if sells:
                    pnl = sell_revenue - buy_cost - buy_fees - sell_fees
                    status = "sold"
                elif result in ("yes", "no"):
                    s = settlements.get(ticker, {})
                    rev = s.get("revenue", 0)
                    if isinstance(rev, (int, float)) and rev > 100:
                        rev = rev / 100
                    fee = float(s.get("fee_cost", "0") or "0")
                    pnl = rev - buy_cost - buy_fees - fee
                    status = "settled"
                else:
                    pnl = 0
                    status = "open"

                trades.append({
                    "ticker": ticker,
                    "title": title,
                    "sub_title": sub,
                    "status": status,
                    "result": result,
                    "contracts": total_contracts,
                    "buy_cost": round(buy_cost, 2),
                    "total_fees": round(buy_fees + sell_fees, 2),
                    "sell_revenue": round(sell_revenue, 2) if sells else None,
                    "pnl": round(pnl, 2),
                    "date": market_fills[0].get("created_time", "")[:10],
                })

        trades.sort(key=lambda t: t.get("date", ""), reverse=True)
        total_pnl = sum(t["pnl"] for t in trades if t["status"] != "open")
        wins = sum(1 for t in trades if t["pnl"] > 0 and t["status"] != "open")
        losses = sum(1 for t in trades if t["pnl"] < 0 and t["status"] != "open")

        return JSONResponse({
            "trades": trades,
            "total_pnl": round(total_pnl, 2),
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / max(wins + losses, 1) * 100, 1),
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/whales")
async def get_whales():
    return JSONResponse(store.snapshot())


@app.get("/api/follower")
async def get_follower_status():
    return JSONResponse(follower.get_status())


@app.get("/api/signals")
async def get_signals():
    return JSONResponse({
        "signals": [s.to_dict() for s in follower.signals_log[-25:]],
        "count": len(follower.signals_log),
    })


@app.get("/api/portfolio")
async def get_portfolio():
    """Fetch real Kalshi portfolio — live positions + P&L."""
    try:
        from src.clients.kalshi_client import KalshiClient
        client = KalshiClient(
            api_key="5ed556bd-2c97-46f4-bf57-e3e8891ffdb4",
            private_key_path="kalshi_private_key.pem",
        )
        balance_data = await client.get_balance()
        positions_data = await client.get_positions()
        await client.close()

        # Enrich positions with market data
        positions = []
        for p in positions_data.get("market_positions", []):
            ticker = p.get("ticker", "")
            contracts = float(p.get("position_fp", "0") or "0")
            cost = float(
                p.get("market_exposure_dollars")
                or p.get("total_cost_dollars")
                or "0"
            )
            fees = float(p.get("fees_paid_dollars", "0") or "0")

            # Get live market price + title
            try:
                async with httpx.AsyncClient(timeout=10.0) as http:
                    mr = await http.get(f"{KALSHI_BASE}{MARKET_ENDPOINT}/{ticker}")
                    md = mr.json().get("market", mr.json())
                    title = md.get("title", ticker)
                    yes_bid = float(md.get("yes_bid_dollars", "0") or "0")
                    no_bid = float(md.get("no_bid_dollars", "0") or "0")
                    yes_sub = md.get("yes_sub_title", "")
                    no_sub = md.get("no_sub_title", "")
                    status = md.get("status", "")
                    result = md.get("result", "")
            except Exception:
                title = ticker
                yes_bid = 0
                no_bid = 0
                yes_sub = ""
                no_sub = ""
                status = ""
                result = ""

            # Skip closed/empty positions
            if contracts == 0:
                continue

            # Determine side from ticker or position
            side = "yes" if contracts > 0 else "no"
            abs_contracts = abs(contracts)
            current_price = yes_bid if side == "yes" else no_bid
            current_value = abs_contracts * current_price
            # Real P&L matching Kalshi:
            # Sell fees ~3.65 cents/contract, buy fees already in fees_paid
            sell_fees_est = abs_contracts * 0.0365
            net_sell_value = current_value - sell_fees_est
            total_cost_basis = cost + fees  # exposure + buy fees
            pnl = net_sell_value - total_cost_basis

            positions.append({
                "ticker": ticker,
                "title": title,
                "side": side,
                "contracts": abs_contracts,
                "cost": round(total_cost_basis, 2),
                "fees": round(fees + sell_fees_est, 2),
                "current_price": current_price,
                "current_value": round(net_sell_value, 2),
                "pnl": round(pnl, 2),
                "pnl_pct": round((pnl / total_cost_basis * 100) if total_cost_basis > 0 else 0, 1),
                "yes_sub_title": yes_sub,
                "no_sub_title": no_sub,
                "status": status,
                "result": result,
            })

        cash = balance_data.get("balance", 0) / 100  # cents to dollars
        portfolio_value = balance_data.get("portfolio_value", 0) / 100
        total_pnl = sum(p["pnl"] for p in positions)

        return JSONResponse({
            "cash": round(cash, 2),
            "portfolio_value": round(portfolio_value, 2),
            "total_pnl": round(total_pnl, 2),
            "positions": positions,
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "whales_tracked": len(store.whales),
        "follower_mode": "paper" if follower.config.paper_mode else "live",
        "open_positions": sum(1 for p in follower.positions if p.status == "open"),
    }
