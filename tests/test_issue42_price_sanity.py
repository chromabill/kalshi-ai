#!/usr/bin/env python3
"""
Tests for Issue #42 fix: price sanity checks for collection/aggregate tickers.

Collection tickers (e.g. KXMVECROSSCATEGORY-*, KXMVESPORTSMULTIGAMEEXTENDED-*)
are not directly tradeable on Kalshi.  The API returns yes_ask == no_ask == $1.00
for these markets.  Placing an order against them yields HTTP 400 invalid_price.

Fixes:
  - src/utils/market_prices.py: new is_tradeable_market() helper
  - src/jobs/execute.py: guards that skip non-tradeable / out-of-range prices
  - src/jobs/ingest.py: uses is_tradeable_market() instead of inline check
"""
import asyncio
import os
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, Mock

from src.utils.market_prices import is_tradeable_market, get_market_prices
from src.utils.database import Position

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# is_tradeable_market() unit tests
# ---------------------------------------------------------------------------

class TestIsTradeableMarket:
    """Unit tests for the is_tradeable_market helper."""

    def _v2_market(self, yes_ask: float, no_ask: float) -> dict:
        """Build a minimal API-v2 market dict."""
        return {
            "yes_bid_dollars": 0.0,
            "yes_ask_dollars": yes_ask,
            "no_bid_dollars": 0.0,
            "no_ask_dollars": no_ask,
        }

    def _legacy_market(self, yes_ask_cents: int, no_ask_cents: int) -> dict:
        """Build a minimal legacy (cent-based) market dict."""
        return {
            "yes_bid": 0,
            "yes_ask": yes_ask_cents,
            "no_bid": 0,
            "no_ask": no_ask_cents,
        }

    # --- Collection / aggregate tickers (should be rejected) ---

    def test_collection_ticker_v2_both_at_1(self):
        """API v2: yes_ask=1.00, no_ask=1.00 → not tradeable."""
        market = self._v2_market(yes_ask=1.0, no_ask=1.0)
        assert is_tradeable_market(market) is False

    def test_collection_ticker_v2_both_at_threshold(self):
        """API v2: yes_ask=0.99, no_ask=0.99 → not tradeable (boundary)."""
        market = self._v2_market(yes_ask=0.99, no_ask=0.99)
        assert is_tradeable_market(market) is False

    def test_collection_ticker_legacy_both_at_100(self):
        """Legacy: yes_ask=100¢, no_ask=100¢ → not tradeable."""
        market = self._legacy_market(yes_ask_cents=100, no_ask_cents=100)
        assert is_tradeable_market(market) is False

    def test_collection_ticker_legacy_both_at_99(self):
        """Legacy: yes_ask=99¢, no_ask=99¢ → not tradeable (boundary)."""
        market = self._legacy_market(yes_ask_cents=99, no_ask_cents=99)
        assert is_tradeable_market(market) is False

    # --- Normal tradeable markets (should pass) ---

    def test_normal_market_v2(self):
        """Typical 50/50 market is tradeable."""
        market = self._v2_market(yes_ask=0.52, no_ask=0.50)
        assert is_tradeable_market(market) is True

    def test_normal_market_legacy(self):
        """Typical legacy 50/50 market is tradeable."""
        market = self._legacy_market(yes_ask_cents=52, no_ask_cents=50)
        assert is_tradeable_market(market) is True

    def test_only_yes_ask_high(self):
        """Only yes_ask is high → still tradeable (no_ask is normal)."""
        market = self._v2_market(yes_ask=0.99, no_ask=0.05)
        assert is_tradeable_market(market) is True

    def test_only_no_ask_high(self):
        """Only no_ask is high → still tradeable (yes_ask is normal)."""
        market = self._v2_market(yes_ask=0.05, no_ask=0.99)
        assert is_tradeable_market(market) is True

    def test_just_below_threshold(self):
        """Both asks at 0.9899 → tradeable (below threshold)."""
        market = self._v2_market(yes_ask=0.9899, no_ask=0.9899)
        assert is_tradeable_market(market) is True


# ---------------------------------------------------------------------------
# execute_position() integration tests — price guard paths
# ---------------------------------------------------------------------------

def _make_position(market_id: str = "TEST-MARKET", side: str = "YES") -> Position:
    return Position(
        market_id=market_id,
        side=side,
        entry_price=0.50,
        quantity=5,
        timestamp=datetime.now(),
        rationale="Issue #42 test",
        confidence=0.75,
        live=False,
        id=1,
    )


def _mock_clients(market_prices: dict):
    """Return (db_manager_mock, kalshi_client_mock) wired with the given market data."""
    db_mock = AsyncMock()
    db_mock.update_position_to_live = AsyncMock(return_value=None)

    kalshi_mock = Mock()
    kalshi_mock.get_market = AsyncMock(return_value={"market": market_prices})
    kalshi_mock.place_order = AsyncMock(return_value={"order": {"order_id": "ord-test-42"}})
    return db_mock, kalshi_mock


async def test_execute_skips_collection_ticker_yes_side():
    """execute_position returns False and does NOT place an order for a collection ticker."""
    from src.jobs.execute import execute_position

    collection_market = {
        "yes_bid_dollars": 0.0,
        "yes_ask_dollars": 1.0,
        "no_bid_dollars": 0.0,
        "no_ask_dollars": 1.0,
    }
    db_mock, kalshi_mock = _mock_clients(collection_market)
    position = _make_position(side="YES")

    result = await execute_position(
        position=position,
        live_mode=True,
        db_manager=db_mock,
        kalshi_client=kalshi_mock,
    )

    assert result is False, "Should return False for collection ticker"
    kalshi_mock.place_order.assert_not_called()


async def test_execute_skips_collection_ticker_no_side():
    """execute_position returns False for NO-side collection ticker too."""
    from src.jobs.execute import execute_position

    collection_market = {
        "yes_bid_dollars": 0.0,
        "yes_ask_dollars": 1.0,
        "no_bid_dollars": 0.0,
        "no_ask_dollars": 1.0,
    }
    db_mock, kalshi_mock = _mock_clients(collection_market)
    position = _make_position(side="NO")

    result = await execute_position(
        position=position,
        live_mode=True,
        db_manager=db_mock,
        kalshi_client=kalshi_mock,
    )

    assert result is False, "Should return False for NO-side collection ticker"
    kalshi_mock.place_order.assert_not_called()


async def test_execute_skips_zero_price():
    """execute_position returns False when ask converts to 0¢."""
    from src.jobs.execute import execute_position

    zero_market = {
        "yes_bid_dollars": 0.0,
        "yes_ask_dollars": 0.001,   # rounds to 0¢
        "no_bid_dollars": 0.0,
        "no_ask_dollars": 0.50,
    }
    db_mock, kalshi_mock = _mock_clients(zero_market)
    position = _make_position(side="YES")

    result = await execute_position(
        position=position,
        live_mode=True,
        db_manager=db_mock,
        kalshi_client=kalshi_mock,
    )

    assert result is False, "Should return False when ask_cents == 0"
    kalshi_mock.place_order.assert_not_called()


async def test_execute_skips_100_cent_price():
    """execute_position returns False when ask converts to 100¢ for one side."""
    from src.jobs.execute import execute_position

    # YES ask is 0.995 which rounds to 100¢; NO ask is normal so is_tradeable passes.
    # The per-side cents guard should catch it.
    boundary_market = {
        "yes_bid_dollars": 0.0,
        "yes_ask_dollars": 0.995,   # rounds to 100¢ (>= 100 guard)
        "no_bid_dollars": 0.0,
        "no_ask_dollars": 0.50,     # normal — so is_tradeable_market passes
    }
    db_mock, kalshi_mock = _mock_clients(boundary_market)
    position = _make_position(side="YES")

    result = await execute_position(
        position=position,
        live_mode=True,
        db_manager=db_mock,
        kalshi_client=kalshi_mock,
    )

    assert result is False, "Should return False when yes_ask_cents >= 100"
    kalshi_mock.place_order.assert_not_called()


async def test_execute_allows_normal_market():
    """execute_position proceeds normally for a healthy 50/50 market."""
    from src.jobs.execute import execute_position

    normal_market = {
        "yes_bid_dollars": 0.48,
        "yes_ask_dollars": 0.52,
        "no_bid_dollars": 0.46,
        "no_ask_dollars": 0.50,
    }
    db_mock, kalshi_mock = _mock_clients(normal_market)
    position = _make_position(side="YES")

    result = await execute_position(
        position=position,
        live_mode=True,
        db_manager=db_mock,
        kalshi_client=kalshi_mock,
    )

    assert result is True, "Should succeed for a normal tradeable market"
    kalshi_mock.place_order.assert_called_once()
