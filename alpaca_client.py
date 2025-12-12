import httpx
from typing import Optional, Tuple
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

def get_order_status(
    order_id: str
) -> Tuple[
    Optional[str],        # status
    Optional[float],      # filled_price
    Optional[str],        # filled_time (ISO string)
    Optional[int],        # error_code
    Optional[str],        # error_message
]:

#def get_order_status(order_id: str) -> Tuple[Optional[str], Optional[float], Optional[int], Optional[str]]:
    """
    Fetch the current status of an Alpaca order by its order_id.

    Returns:
        (status, filled_avg_price, error_code, error_message)

        - status: e.g. 'new', 'accepted', 'pending_new', 'partially_filled',
                  'filled', 'canceled', 'rejected', 'expired', etc.
                  None if we couldn't fetch a valid response.
        - filled_avg_price: float or None (from 'filled_avg_price' in the order)
        - error_code: HTTP status code (int) on error, else None.
        - error_message: Short error message/text on error, else None.
    """
    if not order_id:
        return None, None, None, "empty order_id"

    url = f"{_order_url()}/{order_id}"

    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.get(url, headers=_headers())
            resp.raise_for_status()
            data = resp.json() or {}
            status = data.get("status")

            price_raw = data.get("filled_avg_price")
            filled_price = float(price_raw) if price_raw is not None else None
            
            # Alpaca provides filled_at as ISO8601 when filled
            filled_time = data.get("filled_at")  # keep as ISO string


            if not status:
                log(
                    "error",
                    "alpaca_get_order_no_status",
                    order_id=order_id,
                    raw=data,
                )

            return status, filled_price, filled_time, None, None

    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code if e.response is not None else None
        text = e.response.text if e.response is not None else str(e)
        short_text = (text or "")[:250]

        log(
            "error",
            "alpaca_get_order_http_error",
            order_id=order_id,
            status_code=status_code,
            response_text=short_text,
        )
        return None, None, None, status_code, short_text

    except Exception as e:
        msg = str(e)[:250]
        log(
            "error",
            "alpaca_get_order_other_error",
            order_id=order_id,
            error=msg,
        )
        return None, None, None, None, msg


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



def place_equity_market(
    symbol: str,
    qty: int,
    side: str,
) -> Tuple[Optional[float], Optional[str], Optional[int], Optional[str]]:
    """
    Place a market order for an equity via Alpaca PAPER account.

    - symbol: underlying ticker, e.g. "SPY"
    - qty: share quantity
    - side: "buy" or "sell"

    Returns:
        (fill_price, order_id, error_code, error_message)

        - fill_price: Approximate fill price (float) if available, else None.
        - order_id: Alpaca order id (str) if the order was accepted, else None.
        - error_code: HTTP status code (int) on error, else None.
        - error_message: Short error message/text on error, else None.
    """
    url = _order_url()

    side_norm = (side or "").lower()
    if side_norm not in ("buy", "sell"):
        msg = f"invalid side: {side}"
        log(
            "error",
            "alpaca_equity_invalid_side",
            symbol=symbol,
            qty=qty,
            side=side,
            error=msg,
        )
        return None, None, 400, msg  # treat as client-side fatal error

    data = {
        "symbol": symbol,
        "qty": qty,
        "side": side_norm,
        "type": "market",
        "time_in_force": "day",
    }

    payload = None
    try:
        with httpx.Client(timeout=8.0) as client:
            resp = client.post(url, headers=_headers(), json=data)
            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                # Category: HTTP error (400, 401, 403, 422, 429, 500, etc.)
                status_code = resp.status_code
                # Keep message short-ish for comment/log usage
                text = resp.text or str(e)
                short_text = text[:250]

                log(
                    "error",
                    "alpaca_equity_http_error",
                    symbol=symbol,
                    qty=qty,
                    side=side_norm,
                    status_code=status_code,
                    response_text=text,
                    error=str(e),
                )
                return None, None, None, status_code, short_text

            payload = resp.json()
            log("info", "alpaca_equity_raw_payload", payload=payload)
    except Exception as e:
        # Network / client / JSON errors – no HTTP status_code available.
        msg = str(e)
        log(
            "error",
            "alpaca_equity_order_error",
            symbol=symbol,
            qty=qty,
            side=side_norm,
            error=msg,
        )
        # error_code = None -> manager can decide whether to treat as fatal/soft
        return None, None, None, None, msg

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

    fill_price = _extract_fill_price(payload or {})
    order_id = (payload or {}).get("id")

    # Success path: no HTTP error
    return fill_price, order_id, None, None



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


def place_option_market(
    occ: str,
    qty: int,
    side: str,
) -> Tuple[Optional[float], Optional[str], Optional[int], Optional[str]]:
    """
    Place a market order for an option via Alpaca PAPER account.

    - occ: OCC-style symbol, e.g. "AMD260102P00180000" or "O:AMD260102P00180000"
    - qty: contract quantity
    - side: "buy_to_open", "sell_to_close", etc. (mapped internally to "buy"/"sell")

    Returns:
        (fill_price, order_id, error_code, error_message)

        - fill_price: Approximate fill price (float) if available, else None.
        - order_id: Alpaca order id (str) if the order was accepted, else None.
        - error_code: HTTP status code (int) on error, else None.
        - error_message: Short error message/text on error, else None.
    """

    # Skip placing options MARKET orders outside regular market hours.
    if not _is_market_open_now():
        msg = "market_closed_for_option_market_order"
        log(
            "info",
            "alpaca_option_skipped_market_closed",
            occ=occ,
            qty=qty,
            side=side,
        )
        # No HTTP error here; manager can treat this as soft/no-op if desired
        return None, None, None, None, msg

    url = _order_url()
    occ_clean = _normalize_occ(occ)
    side_norm = _map_option_side(side)

    if not occ_clean:
        msg = "missing OCC symbol"
        log("error", "alpaca_option_missing_symbol", occ=occ, qty=qty, side=side)
        return None, None, 400, msg

    if side_norm is None:
        msg = f"invalid side: {side}"
        log("error", "alpaca_option_invalid_side", occ=occ, qty=qty, side=side)
        return None, None, 400, msg

    data = {
        "symbol": occ_clean,
        "qty": qty,
        "side": side_norm,
        "type": "market",
        "time_in_force": "day",
        # make it explicit we're dealing with options
        "asset_class": "option",
    }

    payload = None
    try:
        with httpx.Client(timeout=8.0) as client:
            resp = client.post(url, headers=_headers(), json=data)
            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                status_code = resp.status_code
                text = resp.text or str(e)
                short_text = text[:250]

                # Log HTTP status + Alpaca response body for debugging
                log(
                    "error",
                    "alpaca_option_http_error",
                    occ=occ,
                    qty=qty,
                    side=side,
                    status_code=status_code,
                    response_text=text,
                    error=str(e),
                )
                return None, None, None, status_code, short_text

            payload = resp.json()
            log("info", "alpaca_option_raw_payload", payload=payload)
    except Exception as e:
        msg = str(e)
        log(
            "error",
            "alpaca_option_order_error",
            occ=occ,
            qty=qty,
            side=side,
            error=msg,
        )
        return None, None, None, None, msg

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

    fill_price = _extract_fill_price(payload or {})
    order_id = (payload or {}).get("id")

    return fill_price, order_id, None, None

