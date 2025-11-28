import httpx
from typing import Optional
from datetime import datetime, time
from zoneinfo import ZoneInfo

from config import settings
from logger import log


def _is_market_open_now() -> bool:
    """
    Return True if it's regular market hours in New York (Mon–Fri, 9:30–16:00 ET).
    No holiday calendar – just weekday + time.
    """
    try:
        now = datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        # Fallback: if timezone fails for some reason, assume open
        now = datetime.utcnow()

    # 0 = Monday, 6 = Sunday
    if now.weekday() >= 5:
        return False

    t = now.time()
    # Regular hours only – you can adjust if you want pre/postmarket
    return time(9, 30) <= t <= time(16, 0)


def _headers() -> dict:
    """
    Base headers for Alpaca trading API.
    We always send JSON for order placement.
    """
    return {
        "APCA-API-KEY-ID": settings.alpaca_key or "",
        "APCA-API-SECRET-KEY": settings.alpaca_secret or "",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _order_url() -> str:
    """
    Build the Alpaca orders endpoint from ALPACA_BASE.

    Example:
      ALPACA_BASE = https://paper-api.alpaca.markets

      => https://paper-api.alpaca.markets/v2/orders
    """
    base = (settings.alpaca_base or "").rstrip("/")
    return f"{base}/v2/orders"


def _extract_fill_price(order: dict) -> Optional[float]:
    """
    Try to extract a reasonable fill price from an Alpaca order response.

    For the paper API we primarily look at:
      - filled_avg_price
      - avg_price
      - limit_price (fallback)
    """
    if not isinstance(order, dict):
        return None

    price = (
        order.get("filled_avg_price")
        or order.get("avg_price")
        or order.get("limit_price")
    )
    if price is None:
        return None

    try:
        return float(price)
    except Exception:
        return None


def place_equity_market(symbol: str, qty: int, side: str) -> Optional[float]:
    """
    Place a market order for an equity via Alpaca PAPER account.

    - symbol: underlying ticker, e.g. "SPY"
    - qty: share quantity
    - side: "buy" or "sell"

    Returns:
        Approximate fill price (float) if available, else None.
        Caller can fall back to signal price if needed.
    """
    url = _order_url()

    side_norm = (side or "").lower()
    if side_norm not in ("buy", "sell"):
        log(
            "error",
            "alpaca_equity_invalid_side",
            symbol=symbol,
            qty=qty,
            side=side,
        )
        return None

    data = {
        "symbol": symbol,
        "qty": qty,
        "side": side_norm,
        "type": "market",
        "time_in_force": "day",
    }
    
    try:
        with httpx.Client(timeout=8.0) as client:
            resp = client.post(url, headers=_headers(), json=data)
            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                # Log HTTP status + Alpaca response body for debugging (403, 422, etc.)
                log(
                    "error",
                    "alpaca_equity_http_error",
                    symbol=symbol,
                    qty=qty,
                    side=side_norm,
                    status_code=resp.status_code,
                    response_text=resp.text,
                    error=str(e),
                )
                return None

            payload = resp.json()
            log("info", "alpaca_equity_raw_payload", payload)
    except Exception as e:
        log(
            "error",
            "alpaca_equity_order_error",
            symbol=symbol,
            qty=qty,
            side=side_norm,
            error=str(e),
        )
        return None

    status = (payload or {}).get("status")
    if status not in (
        "filled",
        "partially_filled",
        "accepted",
        "new",
        "pending_new",
    ):
        # We still try to parse a price, but log that status is unexpected
        log(
            "error",
            "alpaca_equity_order_unexpected_status",
            symbol=symbol,
            qty=qty,
            side=side_norm,
            status=status,
            raw=payload,
        )

    return _extract_fill_price(payload or {})


def _normalize_occ(occ: str) -> str:
    """
    Normalize OCC symbol:

    - If stored as "O:AMD260102P00180000", strip the "O:" prefix.
    - Otherwise return as-is.

    Alpaca expects the raw OCC-like string (no "O:" prefix).
    """
    if not occ:
        return ""
    return occ[2:] if occ.startswith("O:") else occ


def _map_option_side(side: str) -> Optional[str]:
    """
    Map Tradier-style option sides to Alpaca sides.

    Incoming (from our trade_manager):
      - "buy_to_open"
      - "sell_to_close"
      - "sell_to_open"
      - "buy_to_close"
      - or already "buy"/"sell"

    Alpaca:
      - "buy"
      - "sell"
    """
    s = (side or "").lower()

    if s in ("buy", "buy_to_open", "buy_to_close"):
        return "buy"
    if s in ("sell", "sell_to_open", "sell_to_close"):
        return "sell"

    return None


def place_option_market(occ: str, qty: int, side: str) -> Optional[float]:
    """
    Place a market order for an option via Alpaca PAPER account.

    - occ: OCC-style symbol, e.g. "AMD260102P00180000" or "O:AMD260102P00180000"
    - qty: contract quantity
    - side: "buy_to_open", "sell_to_close", etc. (mapped internally to "buy"/"sell")

    Returns:
        Approximate fill price (float) if available, else None.
    """

    # Skip placing options MARKET orders outside regular market hours.
    if not _is_market_open_now():
        log(
            "info",
            "alpaca_option_skipped_market_closed",
            occ=occ,
            qty=qty,
            side=side,
        )
        return None
        
    url = _order_url()
    occ_clean = _normalize_occ(occ)
    side_norm = _map_option_side(side)

    if not occ_clean:
        log("error", "alpaca_option_missing_symbol", occ=occ, qty=qty, side=side)
        return None

    if side_norm is None:
        log("error", "alpaca_option_invalid_side", occ=occ, qty=qty, side=side)
        return None

    data = {
        "symbol": occ_clean,
        "qty": qty,
        "side": side_norm,
        "type": "market",
        "time_in_force": "day",
        # make it explicit we're dealing with options
        "asset_class": "option",
    }
    
    try:
        with httpx.Client(timeout=8.0) as client:
            resp = client.post(url, headers=_headers(), json=data)
            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                # Log HTTP status + Alpaca response body for debugging
                log(
                    "error",
                    "alpaca_option_http_error",
                    occ=occ,
                    qty=qty,
                    side=side,
                    status_code=resp.status_code,
                    response_text=resp.text,
                    error=str(e),
                )
                return None

            payload = resp.json()
            log("info", "alpaca_equity_raw_payload", payload)
    except Exception as e:
        log(
            "error",
            "alpaca_option_order_error",
            occ=occ,
            qty=qty,
            side=side,
            error=str(e),
        )
        return None


    status = (payload or {}).get("status")
    if status not in (
        "filled",
        "partially_filled",
        "accepted",
        "new",
        "pending_new",
    ):
        log(
            "error",
            "alpaca_option_order_unexpected_status",
            occ=occ,
            qty=qty,
            side=side,
            status=status,
            raw=payload,
        )

    return _extract_fill_price(payload or {})
