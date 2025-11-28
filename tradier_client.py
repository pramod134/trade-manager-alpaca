import httpx
from typing import Optional

from config import settings
from logger import log


def _headers() -> dict:
    """
    Base headers for Tradier. We set Content-Type per request so that
    order placement can use form-encoded bodies.
    """
    return {
        "Authorization": f"Bearer {settings.tradier_live_token}",
        "Accept": "application/json",
    }


def _order_url() -> str:
    """
    Build the Tradier orders endpoint from TRADIER_LIVE_BASE and TRADIER_ACCOUNT_ID.

    Examples:
      TRADIER_LIVE_BASE = https://sandbox.tradier.com
      TRADIER_ACCOUNT_ID = VA56300150

      => https://sandbox.tradier.com/v1/accounts/VA56300150/orders
    """
    base = (settings.tradier_live_base or "").rstrip("/")
    return f"{base}/v1/accounts/{settings.tradier_account_id}/orders"


def _extract_fill_price(order: dict) -> Optional[float]:
    """
    Try to extract a reasonable fill price from a Tradier order response.
    We first look at the first fill's price, then average_fill_price.
    """
    if not isinstance(order, dict):
        return None

    fills = order.get("fills") or []
    if fills:
        price = fills[0].get("price")
        if price is not None:
            try:
                return float(price)
            except Exception:
                pass

    avg = order.get("average_fill_price")
    if avg is not None:
        try:
            return float(avg)
        except Exception:
            pass

    return None


def place_equity_market(symbol: str, qty: int, side: str) -> Optional[float]:
    """
    Place a market order for an equity.

    - symbol: underlying ticker, e.g. "SPY"
    - qty: share quantity
    - side: "buy", "sell", "sell_short", or "buy_to_cover"

    Returns:
        Approximate fill price (float) if available, else None.
        Caller can fall back to signal price if needed.
    """
    url = _order_url()
    # Tradier expects form-encoded body for orders.
    data = {
        "class": "equity",
        "symbol": symbol,
        "side": side,
        "quantity": str(qty),
        "type": "market",
        "duration": "day",
    }

    try:
        with httpx.Client(timeout=8.0) as client:
            resp = client.post(
                url,
                headers={**_headers(), "Content-Type": "application/x-www-form-urlencoded"},
                data=data,
            )
            resp.raise_for_status()
            payload = resp.json()
    except Exception as e:
        log(
            "error",
            "tradier_equity_order_error",
            symbol=symbol,
            qty=qty,
            side=side,
            error=str(e),
        )
        return None

    order = (payload or {}).get("order") or {}
    status = order.get("status")

    if status != "ok":
        # We got a response, but Tradier did not accept the order as OK.
        log(
            "error",
            "tradier_equity_order_not_ok",
            symbol=symbol,
            qty=qty,
            side=side,
            status=status,
            raw=payload,
        )
        return None

    return _extract_fill_price(order)


def _normalize_occ(occ: str) -> str:
    """
    Normalize OCC symbol:

    - If stored as "O:AMD260102P00180000", strip the "O:" prefix.
    - Otherwise return as-is.
    """
    if not occ:
        return ""
    return occ[2:] if occ.startswith("O:") else occ


def _occ_underlying(occ_clean: str) -> str:
    """
    Extract underlying symbol from a clean OCC string.

    Example:
        "AMD260102P00180000" -> "AMD"
        "GOOGL251212C00330000" -> "GOOGL"

    We take leading alphabetic characters until the first digit.
    """
    if not occ_clean:
        return ""

    i = 0
    n = len(occ_clean)
    while i < n and occ_clean[i].isalpha():
        i += 1
    underlying = occ_clean[:i]
    return underlying or occ_clean


def place_option_market(occ: str, qty: int, side: str) -> Optional[float]:
    """
    Place a market order for an option.

    - occ: OCC-style symbol, e.g. "AMD260102P00180000" or "O:AMD260102P00180000"
    - qty: contract quantity
    - side: "buy_to_open", "sell_to_close", "sell_to_open", or "buy_to_close"

    We send:
        class = option
        symbol = underlying ticker (e.g. "AMD")
        option_symbol = full OCC code without "O:" prefix
    """
    occ_clean = _normalize_occ(occ)
    underlying = _occ_underlying(occ_clean)

    url = _order_url()
    data = {
        "class": "option",
        "symbol": underlying,
        "option_symbol": occ_clean,
        "side": side,
        "quantity": str(qty),
        "type": "market",
        "duration": "day",
    }

    try:
        with httpx.Client(timeout=8.0) as client:
            resp = client.post(
                url,
                headers={**_headers(), "Content-Type": "application/x-www-form-urlencoded"},
                data=data,
            )
            resp.raise_for_status()
            payload = resp.json()
    except Exception as e:
        log(
            "error",
            "tradier_option_order_error",
            occ=occ,
            qty=qty,
            side=side,
            error=str(e),
        )
        return None

    order = (payload or {}).get("order") or {}
    status = order.get("status")

    if status != "ok":
        log(
            "error",
            "tradier_option_order_not_ok",
            occ=occ,
            qty=qty,
            side=side,
            status=status,
            raw=payload,
        )
        return None

    return _extract_fill_price(order)
