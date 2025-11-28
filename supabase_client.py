from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from supabase import create_client, Client

from config import settings
from logger import log


_sb: Optional[Client] = None


def get_client() -> Client:
    global _sb
    if _sb is None:
        _sb = create_client(settings.supabase_url, settings.supabase_key)
    return _sb


def _unwrap_response(res: Any) -> Tuple[Any, Any]:
    if isinstance(res, dict):
        return res.get("data"), res.get("error")
    return getattr(res, "data", None), getattr(res, "error", None)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------- ACTIVE TRADES ----------


def fetch_active_trades() -> List[Dict[str, Any]]:
    """
    Fetch trades that the manager should look at.
    We manage only manage IN ('Y','C').
    """
    sb = get_client()
    data, err = _unwrap_response(
        sb.table("active_trades")
        .select("*")
        .in_("manage", ["Y", "C"])
        .order("created_at")
        .execute()
    )
    if err:
        raise RuntimeError(err)
    return data or []


def mark_as_managing(row_id: str) -> None:
    """
    After entry is executed, set status to nt-managing.
    """
    sb = get_client()
    _, err = _unwrap_response(
        sb.table("active_trades")
        .update({"status": "nt-managing", "updated_at": _now_iso()})
        .eq("id", row_id)
        .execute()
    )
    if err:
        raise RuntimeError(err)


def delete_trade(row_id: str) -> None:
    """
    Remove the trade from active_trades after close or cancel.
    """
    sb = get_client()
    _, err = _unwrap_response(
        sb.table("active_trades")
        .delete()
        .eq("id", row_id)
        .execute()
    )
    if err:
        raise RuntimeError(err)


# ---------- SPOT / CANDLES ----------


def fetch_spot(instrument_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch one spot row by instrument_id.
    instrument_id = symbol for equities, occ/OCC-style for options.
    """
    if not instrument_id:
        return None

    sb = get_client()
    data, err = _unwrap_response(
        sb.table("spot")
        .select("*")
        .eq("instrument_id", instrument_id)
        .limit(1)
        .execute()
    )
    if err:
        raise RuntimeError(err)
    if not data:
        return None
    return data[0]


# ---------- EXECUTED TRADES ----------


def insert_executed_trade_open(row: Dict[str, Any], open_price: float) -> None:
    """
    Create an executed_trades row when a trade actually opens.

    Cost basis is computed as:
      multiplier = 1 for equity, 100 for option
      open_cost_basis = open_price * qty * multiplier
    """
    sb = get_client()

    asset_type = (row.get("asset_type") or "").lower()
    qty = int(row.get("qty") or 0)
    multiplier = 100 if asset_type == "option" else 1

    open_cost_basis = open_price * qty * multiplier

    payload = {
        "active_trade_id": row["id"],
        "trade_type": row.get("trade_type") or "swing",
        "symbol": row["symbol"],
        "occ": row.get("occ"),
        "asset_type": asset_type,
        "qty": qty,
        "open_ts": _now_iso(),
        "open_price": open_price,
        "open_cost_basis": open_cost_basis,
    }

    data, err = _unwrap_response(
        sb.table("executed_trades")
        .insert(payload)
        .execute()
    )
    if err:
        log("error", "executed_trade_open_insert_error", error=err, payload=payload)
        raise RuntimeError(err)


def update_executed_trade_close(
    active_trade_id: str,
    asset_type: str,
    qty: int,
    close_price: float,
    reason: str,
) -> None:
    """
    On trade close (SL/TP/force), update the matching executed_trades row
    with close_price, close_cost_basis, close_ts, and close_reason.

    We assume 1:1 mapping between active_trade_id and executed_trades row.
    """
    sb = get_client()

    asset_type = (asset_type or "").lower()
    multiplier = 100 if asset_type == "option" else 1
    close_cost_basis = close_price * qty * multiplier

    update = {
        "close_ts": _now_iso(),
        "close_price": close_price,
        "close_cost_basis": close_cost_basis,
        "close_reason": reason,
    }

    data, err = _unwrap_response(
        sb.table("executed_trades")
        .update(update)
        .eq("active_trade_id", active_trade_id)
        .execute()
    )
    if err:
        log(
            "error",
            "executed_trade_close_update_error",
            error=err,
            active_trade_id=active_trade_id,
            update=update,
        )
        raise RuntimeError(err)
