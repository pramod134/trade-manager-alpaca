import time as time_module
from datetime import datetime, timezone, time
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo


from config import settings
from logger import log
import supabase_client
import alpaca_client



def _get_spot_price(spot_row: Optional[Dict[str, Any]]) -> Optional[float]:
    if not spot_row:
        return None
    return spot_row.get("last_price")


def _get_tf_close(spot_row: Optional[Dict[str, Any]], tf: Optional[str]) -> Optional[float]:
    if not spot_row or not tf:
        return None
    tf_closes = spot_row.get("tf_closes") or {}
    tf_row = tf_closes.get(tf)
    if not tf_row:
        return None
    return tf_row.get("close")


# PATCH: helper to choose which instrument (equity vs option) to use for
# entry / SL / TP based on *_type fields (entry_type, sl_type, tp_type),
# not asset_type.
def _choose_spot_row(
    row: Dict[str, Any],
    type_field: str,
    spot_under: Optional[Dict[str, Any]],
    spot_option: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    Selects which instrument's spot row to use for price logic.

    type_field: 'equity' or 'option' (from entry_type / sl_type / tp_type).
    Falls back to underlying (equity) if missing/unknown.
    """
    t = (type_field or "").lower()

    if t == "equity":
        return spot_under
    if t == "option":
        return spot_option

    # Fallback: default to underlying
    return spot_under


def _get_sl_level(row: Dict[str, Any]) -> Optional[float]:
    return row.get("sl_level") or row.get("sl")  # support both names just in case


def _get_tp_level(row: Dict[str, Any]) -> Optional[float]:
    return row.get("tp_level") or row.get("tp")  # support both names just in case


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------- MARKET HOURS HELPERS (manager-local) ----------

MARKET_TZ = ZoneInfo("America/New_York")


def _is_regular_market_open_now() -> bool:
    """
    Return True if it's regular *options* market hours in New York.
    We also intentionally skip the first minute (9:30:00–9:30:59)
    to avoid crazy opening spreads.

    Window: Mon–Fri, 09:31–16:00 ET.
    """
    now_et = datetime.now(MARKET_TZ)

    # 0 = Monday ... 6 = Sunday
    if now_et.weekday() >= 5:
        return False

    t = now_et.time()
    return time(9, 31) <= t <= time(16, 0)



# ---------- ENTRY / SL / TP CHECKS ----------

# PATCH: entry now respects entry_type, and always returns the price used for
# decision (so logs can show entry_price even when should_enter=False).
# ---------- ENTRY / SL / TP CHECKS ----------

# PATCH: entry now respects entry_type, and always returns the price used for
# decision (so logs can show entry_price even when should_enter=False).
def check_entry(
    row: Dict[str, Any],
    spot_under: Optional[Dict[str, Any]],
    spot_option: Optional[Dict[str, Any]],
) -> Tuple[bool, Optional[float]]:
    """
    Returns (should_enter, entry_price_used)

    entry_cond:
      - 'now' -> use spot price of entry_type instrument
      - 'ca'  -> TF candle close ABOVE entry_level (for the entry_type instrument)
      - 'cb'  -> TF candle close BELOW entry_level (for the entry_type instrument)
      - 'at'  -> touch-based on spot price (direction from cp/side)
    """

    cond = (row.get("entry_cond") or "").lower()
    if not cond:
        return False, None

    entry_type = (row.get("entry_type") or "equity").lower()
    entry_tf = row.get("entry_tf")
    level = row.get("entry_level")
    asset_type = (row.get("asset_type") or "").lower()
    cp = (row.get("cp") or "").lower()
    side = (row.get("side") or "").lower()

    # no level needed for 'now'
    if cond != "now" and level is None:
        return False, None

    # which instrument's prices to use (equity vs option)
    spot_row = _choose_spot_row(row, entry_type, spot_under, spot_option)
    if not spot_row:
        return False, None

    price: Optional[float] = None

    # ---- price source rules ----
    if cond in ("at", "now"):
        # for 'at' and 'now' we ALWAYS use spot last price
        price = _get_spot_price(spot_row)
    elif cond in ("ca", "cb"):
        # for ca/cb we use TF candle close
        if not entry_tf:
            return False, None
        price = _get_tf_close(spot_row, entry_tf)
    else:
        # unsupported condition
        return False, None

    if price is None:
        return False, None

    # ---- immediate entry ----
    if cond == "now":
        # enter immediately at current price
        return True, price

    # ---- touch-based entry ('at') ----
    if cond == "at":
        # Determine direction similar to SL/TP logic
        if asset_type == "option":
            if cp in ("c", "call"):
                profit_when_up = True
            elif cp in ("p", "put"):
                profit_when_up = False
            else:
                # fall back to side when cp is missing/unknown
                profit_when_up = (side != "short")
        else:
            # non-option: use side, default to long if missing
            profit_when_up = (side != "short")

        if profit_when_up:
            # Long / calls: enter when price is at or BELOW level (buy at support)
            should_enter = price <= level
        else:
            # Short / puts: enter when price is at or ABOVE level (sell at resistance)
            should_enter = price >= level

        return should_enter, price

    # ---- candle-close entries (ca/cb) ----
    if cond == "ca":
        should_enter = price > level
        return should_enter, price

    if cond == "cb":
        should_enter = price < level
        return should_enter, price

    # Unknown / unsupported condition
    return False, None


# PATCH: SL now explicitly uses sl_type (equity/option) via _choose_spot_row,
# not asset_type, and returns the price used (for better logging).

def check_sl(
    row: Dict[str, Any],
    spot_under: Optional[Dict[str, Any]],
    spot_option: Optional[Dict[str, Any]],
) -> Tuple[bool, Optional[float]]:
    """
    Returns (sl_hit, price_used)

    sl_cond semantics:
      - 'at'  -> level-based SL on spot price (direction from cp for options, otherwise side)
      - 'now' -> immediate SL at current spot price
      - 'ca'  -> TF candle close ABOVE level (for sl_type instrument)
      - 'cb'  -> TF candle close BELOW level (for sl_type instrument)
    """

    enabled = row.get("sl_enabled")
    if enabled is False:
        return False, None

    cond = (row.get("sl_cond") or "").lower()
    if not cond:
        return False, None

    sl_type = (row.get("sl_type") or "equity").lower()
    sl_tf = row.get("sl_tf")
    level = _get_sl_level(row)
    asset_type = (row.get("asset_type") or "").lower()
    cp = (row.get("cp") or "").lower()
    side = (row.get("side") or "").lower()

    # no level needed for 'now'
    if cond != "now" and level is None:
        return False, None

    # which instrument's prices to use (equity vs option)
    spot_row = _choose_spot_row(row, sl_type, spot_under, spot_option)
    if not spot_row:
        return False, None

    price: Optional[float] = None

    # ---- price source rules ----
    if cond in ("at", "now"):
        # for 'at' and 'now' we ALWAYS use spot last price
        price = _get_spot_price(spot_row)
    elif cond in ("ca", "cb"):
        # for ca/cb we use TF candle close
        if not sl_tf:
            return False, None
        price = _get_tf_close(spot_row, sl_tf)
    else:
        # unsupported condition
        return False, None

    if price is None:
        return False, None

    # ---- immediate SL ----
    if cond == "now":
        # close immediately at current price
        return True, price

    # ---- direction logic for 'at' (tick-based SL) ----
    if cond == "at":
        # For options: use cp to infer direction (call vs put)
        if asset_type == "option":
            if cp in ("c", "call"):
                profit_when_up = True
            elif cp in ("p", "put"):
                profit_when_up = False
            else:
                # unknown cp, fall back to side
                profit_when_up = (side != "short")
        else:
            # non-option: use side, default to long if missing
            profit_when_up = (side != "short")

        if profit_when_up:
            # Calls / long: SL when price goes DOWN below level
            sl_hit = price <= level
        else:
            # Puts / short: SL when price goes UP above level
            sl_hit = price >= level

        return sl_hit, price

    # ---- candle-close SL (direction is encoded by ca/cb itself) ----
    if cond == "ca":  # candle close ABOVE level
        return (price > level), price

    if cond == "cb":  # candle close BELOW level
        return (price < level), price

    return False, price



# PATCH: TP now explicitly uses tp_type (equity/option) via _choose_spot_row,
# not asset_type, and returns the price used (for better logging).

def check_tp(
    row: Dict[str, Any],
    spot_under: Optional[Dict[str, Any]],
    spot_option: Optional[Dict[str, Any]],
) -> Tuple[bool, Optional[float]]:
    """
    Returns (tp_hit, price_used)

    TP is always based on spot (last) price of tp_type instrument.

    Direction logic:
      - For options:
          cp = 'c' (call) -> profit when price goes UP  -> hit when price >= tp_level
          cp = 'p' (put)  -> profit when price goes DOWN -> hit when price <= tp_level
      - For non-options or missing cp:
          side = 'long'  -> profit when price goes UP  -> hit when price >= tp_level
          side = 'short' -> profit when price goes DOWN -> hit when price <= tp_level
    """

    enabled = row.get("tp_enabled")
    if enabled is False:
        return False, None

    level = _get_tp_level(row)
    if level is None:
        return False, None

    asset_type = (row.get("asset_type") or "").lower()
    cp = (row.get("cp") or "").lower()
    side = (row.get("side") or "").lower()
    tp_type = (row.get("tp_type") or "equity").lower()

    # Decide whether profit is when price moves UP or DOWN.
    # For options we prefer cp; otherwise fall back to side.
    if asset_type == "option":
        if cp in ("c", "call"):
            profit_when_up = True
        elif cp in ("p", "put"):
            profit_when_up = False
        else:
            # Unknown cp; fall back to side
            profit_when_up = (side != "short")
    else:
        # Non-option: use side if available (default to long)
        profit_when_up = (side != "short")

    # choose equity vs option for TP
    spot_row = _choose_spot_row(row, tp_type, spot_under, spot_option)
    if not spot_row:
        return False, None

    # always use spot last price for TP
    price = _get_spot_price(spot_row)
    if price is None:
        return False, None

    if profit_when_up:
        tp_hit = price >= level
    else:
        tp_hit = price <= level

    return tp_hit, price

# ---------- MAIN LOOP ----------


def run_trade_manager() -> None:
    log("info", "trade_manager_start", interval=settings.trade_manager_interval)

    while True:
        try:
            rows = supabase_client.fetch_active_trades()
        except Exception as e:
            log("error", "tm_fetch_active_trades_error", error=str(e))
            time_module.sleep(settings.trade_manager_interval)
            continue

        for row in rows:
            row_id = row["id"]
            manage = row.get("manage")
            status = row.get("status")
            symbol = row.get("symbol")
            occ = row.get("occ")
            asset_type = (row.get("asset_type") or "").lower()
            entry_type = (row.get("entry_type") or "").lower()
            sl_type = (row.get("sl_type") or "").lower()
            tp_type = (row.get("tp_type") or "").lower()
            qty = int(row.get("qty") or 0)

            # New: broker-order metadata from DB
            order_id = row.get("order_id")
            order_status = (row.get("order_status") or "").lower()
            order_comment = row.get("comment")

            log(
                "debug",
                "tm_row_context",
                id=row_id,
                symbol=symbol,
                occ=occ,
                manage=manage,
                status=status,
                asset_type=asset_type,
                entry_type=entry_type,
                sl_type=sl_type,
                tp_type=tp_type,
                qty=qty,
                order_id=order_id,
                order_status=order_status,
                order_comment=order_comment,
            )

            # ---------- AUTO-PROMOTE FILLED ENTRIES ----------
            # If Alpaca/WebSocket already marked the order as filled but our
            # trade status is still 'nt-waiting', promote it to 'nt-managing'
            # so we don't send a second entry order. From this loop onward,
            # SL/TP logic will manage the position.
            if status == "nt-waiting" and order_id and order_status == "filled":
                log(
                    "info",
                    "tm_entry_already_filled_promote",
                    id=row_id,
                    symbol=symbol,
                    order_id=order_id,
                    old_status=status,
                    new_status="nt-managing",
                )
                try:
                    sb = supabase_client.get_client()
                    sb.table("active_trades").update(
                        {"status": "nt-managing"}
                    ).eq("id", row_id).execute()
                    status = "nt-managing"
                except Exception as e:
                    log(
                        "error",
                        "tm_entry_promote_managing_update_error",
                        id=row_id,
                        symbol=symbol,
                        order_id=order_id,
                        error=str(e),
                    )

            # ---------- Fetch spot rows for underlying + option ----------
            spot_under = None
            spot_option = None
          
            try:
                if symbol:
                    spot_under = supabase_client.fetch_spot(symbol)
                if occ:
                    spot_option = supabase_client.fetch_spot(occ)
            except Exception as e:
                log(
                    "error",
                    "tm_fetch_spot_error",
                    id=row_id,
                    symbol=symbol,
                    occ=occ,
                    error=str(e),
                )
                continue

            # Helpers
            def _get_spot_price(spot_row: Optional[dict]) -> Optional[float]:
                if not isinstance(spot_row, dict):
                    return None
                price = spot_row.get("last")
                if price is None:
                    price = spot_row.get("close")
                try:
                    return float(price) if price is not None else None
                except Exception:
                    return None

            terminal_order_statuses = ("filled", "rejected", "canceled", "expired")

            # ---------- MANAGE = 'C' (force close) ----------
            if manage == "C":
                log(
                    "info",
                    "tm_force_close",
                    id=row_id,
                    symbol=symbol,
                    status=status,
                    asset_type=asset_type,
                    qty=qty,
                )

                # If no position yet, just delete the row (no broker trade existed)
                if status == "nt-waiting":
                    log(
                        "info",
                        "tm_force_close_nt_waiting_delete",
                        id=row_id,
                        symbol=symbol,
                    )
                    try:
                        supabase_client.delete_trade(row_id)
                    except Exception as e:
                        log(
                            "error",
                            "tm_force_delete_error",
                            id=row_id,
                            error=str(e),
                        )
                    continue

                # If managing, close via broker, record close price ONLY if we have a fill
                if status in ("nt-managing", "pos-managing"):
                    if asset_type == "equity":
                        signal_price = _get_spot_price(spot_under)
                        log(
                            "debug",
                            "tm_force_close_place_equity",
                            id=row_id,
                            symbol=symbol,
                            qty=qty,
                            signal_price=signal_price,
                        )
                        fill_price, _ = alpaca_client.place_equity_market(
                            symbol, qty, "sell"
                        )
                    else:
                        signal_price = _get_spot_price(spot_option)
                        log(
                            "debug",
                            "tm_force_close_place_option",
                            id=row_id,
                            occ=occ,
                            qty=qty,
                            signal_price=signal_price,
                        )
                        fill_price, _ = alpaca_client.place_option_market(
                            occ, qty, "sell_to_close"
                        )

                    log(
                        "debug",
                        "tm_force_close_result",
                        id=row_id,
                        symbol=symbol,
                        occ=occ,
                        fill_price=fill_price,
                        signal_price=signal_price,
                    )

                    # Only treat as closed if we have a confirmed fill
                    if fill_price is None:
                        log(
                            "error",
                            "tm_force_close_no_fill",
                            id=row_id,
                            symbol=symbol,
                            occ=occ,
                            asset_type=asset_type,
                            qty=qty,
                        )
                    else:
                        close_price = fill_price
                        try:
                            supabase_client.update_executed_trade_close(
                                active_trade_id=row_id,
                                asset_type=asset_type,
                                qty=qty,
                                close_price=close_price,
                                reason="force",
                            )
                        except Exception as e:
                            log(
                                "error",
                                "tm_force_executed_update_error",
                                id=row_id,
                                error=str(e),
                            )

                        try:
                            supabase_client.delete_trade(row_id)
                        except Exception as e:
                            log(
                                "error",
                                "tm_force_delete_error",
                                id=row_id,
                                error=str(e),
                            )

                continue  # done with manage='C'

            # ---------- MANAGE = 'Y' ----------
            if manage != "Y":
                log("debug", "tm_manage_skip", id=row_id, manage=manage)
                continue

            # ---------- STATUS = 'nt-waiting' (new entry) ----------
            if status == "nt-waiting":
                # If an entry order is already working, do NOT send another
                if order_id and order_status not in terminal_order_statuses:
                    log(
                        "debug",
                        "tm_entry_order_pending",
                        id=row_id,
                        symbol=symbol,
                        order_id=order_id,
                        order_status=order_status,
                    )
                    continue

                should_enter, entry_price = check_entry(row, spot_under, spot_option)
                log(
                    "debug",
                    "tm_entry_check",
                    id=row_id,
                    symbol=symbol,
                    should_enter=should_enter,
                    entry_price=entry_price,
                )

                if not should_enter or entry_price is None:
                    continue

                log(
                    "info",
                    "tm_entry_triggered",
                    id=row_id,
                    symbol=symbol,
                    price=entry_price,
                )

                # ---------- PLACE ENTRY ORDER (with error categories) ----------
                new_order_id: Optional[str] = None
                error_code: Optional[int] = None
                error_message: Optional[str] = None

                if asset_type == "equity":
                    log(
                        "debug",
                        "tm_entry_place_equity",
                        id=row_id,
                        symbol=symbol,
                        qty=qty,
                    )
                    fill_price, new_order_id, error_code, error_message = (
                        alpaca_client.place_equity_market(symbol, qty, "buy")
                    )
                else:
                    # For options, only send market orders during regular hours,
                    # and skip the first minute after open (09:30).
                    if not _is_regular_market_open_now():
                        log(
                            "info",
                            "tm_entry_skip_market_closed_option",
                            id=row_id,
                            symbol=symbol,
                            occ=occ,
                            qty=qty,
                        )
                        # Do NOT mark this as error; just try again on next loop.
                        continue

                    log(
                        "debug",
                        "tm_entry_place_option",
                        id=row_id,
                        occ=occ,
                        qty=qty,
                    )
                    fill_price, new_order_id, error_code, error_message = (
                        alpaca_client.place_option_market(occ, qty, "buy_to_open")
                    )

                # ---------- CATEGORY 1: we have an order_id (normal flow) ----------
                if new_order_id:
                    if not order_id:
                        try:
                            sb = supabase_client.get_client()
                            sb.table("active_trades").update(
                                {
                                    "order_id": new_order_id,
                                    "order_status": "pending_new",
                                    "comment": "entry",
                                }
                            ).eq("id", row_id).execute()
                            order_id = new_order_id
                            order_status = "pending_new"
                            order_comment = "entry"
                        except Exception as e:
                            log(
                                "error",
                                "tm_entry_order_meta_update_error",
                                id=row_id,
                                error=str(e),
                            )

                # ---------- CATEGORY 2 & 3: no order_id (hard vs soft error) ----------
                if not new_order_id:
                    # NOTE: error_code may be None if alpaca_client couldn't parse it;
                    # treat unknown as fatal.
                    fatal_codes = {400, 401, 403, 422}
                    soft_codes = {429}

                    is_fatal = (
                        error_code in fatal_codes
                        or error_code is None  # unknown -> be safe
                    )
                    is_soft = (error_code in soft_codes) or (
                        isinstance(error_code, int) and error_code >= 500
                    )

                    if is_fatal:
                        # Category 2: No order_id, fatal error -> hard stop this trade
                        log(
                            "error",
                            "tm_entry_fatal_error",
                            id=row_id,
                            symbol=symbol,
                            http_code=error_code,
                            error=error_message,
                        )
                        try:
                            sb = supabase_client.get_client()
                            safe_msg = (error_message or "")[:150]  # avoid huge alpaca strings
                            sb.table("active_trades").update(
                                {
                                    "order_id": "Error",
                                    "order_status": "error",
                                    "manage": "N",
                                    "comment": f"entry_error_{error_code}: {safe_msg}",
                                }
                            ).eq("id", row_id).execute()
                        except Exception as e:
                            log(
                                "error",
                                "tm_entry_fatal_error_update_failed",
                                id=row_id,
                                error=str(e),
                            )
                        # Do NOT try again; this row is effectively frozen
                        continue

                    if is_soft:
                        # Category 3: No order_id, soft error -> log and allow retry
                        log(
                            "error",
                            "tm_entry_soft_error",
                            id=row_id,
                            symbol=symbol,
                            http_code=error_code,
                            error=error_message,
                        )
                        # No DB changes; on next loop we'll try again if conditions still true
                        continue

                    # Fallback: unknown error with no order_id -> treat as fatal
                    log(
                        "error",
                        "tm_entry_unknown_error_no_order_id",
                        id=row_id,
                        symbol=symbol,
                        http_code=error_code,
                        error=error_message,
                    )
                    try:
                        sb = supabase_client.get_client()
                        safe_msg = (error_message or "")[:150]
                        sb.table("active_trades").update(
                            {
                                "order_id": "Error",
                                "order_status": "error",
                                "manage": "N",                                
                                "comment": f"entry_error_unknown: {safe_msg}",
                            }
                        ).eq("id", row_id).execute()
                    except Exception as e:
                        log(
                            "error",
                            "tm_entry_unknown_error_update_failed",
                            id=row_id,
                            error=str(e),
                        )
                    continue

                # ---------- entry result + open trade ----------
                log(
                    "debug",
                    "tm_entry_result",
                    id=row_id,
                    symbol=symbol,
                    occ=occ,
                    entry_price=entry_price,
                    fill_price=fill_price,
                )

                # Only move to managing if we have a confirmed fill
                if fill_price is None:
                    log(
                        "error",
                        "tm_entry_no_fill",
                        id=row_id,
                        symbol=symbol,
                        occ=occ,
                        asset_type=asset_type,
                        qty=qty,
                    )
                    # We rely on WS to update order_status to 'filled' later; next loop
                    # will see that and we can promote to nt-managing in a separate patch
                    continue

                open_price = fill_price
                try:
                    supabase_client.insert_executed_trade_open(
                        active_trade_row=row,
                        asset_type=asset_type,
                        qty=qty,
                        open_price=open_price,
                    )
                except Exception as e:
                    log(
                        "error",
                        "tm_entry_executed_insert_error",
                        id=row_id,
                        error=str(e),
                    )
                    continue

                try:
                    supabase_client.mark_as_managing(row_id)
                except Exception as e:
                    log(
                        "error",
                        "tm_entry_mark_managing_error",
                        id=row_id,
                        error=str(e),
                    )

                continue  # done with this trade for this loop

            # ---------- STATUS = 'nt-managing' / 'pos-managing' (SL / TP) ----------
            if status in ("nt-managing", "pos-managing"):
                # ---- SL FIRST ----
                sl_hit, sl_price_signal = check_sl(row, spot_under, spot_option)
                log(
                    "debug",
                    "tm_sl_check",
                    id=row_id,
                    symbol=symbol,
                    sl_hit=sl_hit,
                    sl_price_signal=sl_price_signal,
                )

                if sl_hit and sl_price_signal is not None:
                    log(
                        "info",
                        "tm_sl_hit",
                        id=row_id,
                        symbol=symbol,
                        price=sl_price_signal,
                    )

                    # If an SL order is already working, don't send another
                    if order_id and order_status not in terminal_order_statuses:
                        log(
                            "debug",
                            "tm_sl_order_pending",
                            id=row_id,
                            symbol=symbol,
                            order_id=order_id,
                            order_status=order_status,
                        )
                        continue

                    new_order_id: Optional[str] = None
                    error_code: Optional[int] = None
                    error_message: Optional[str] = None

                    if asset_type == "equity":
                        log(
                            "debug",
                            "tm_sl_place_equity",
                            id=row_id,
                            symbol=symbol,
                            qty=qty,
                        )
                        fill_price, new_order_id, error_code, error_message = (
                            alpaca_client.place_equity_market(symbol, qty, "sell")
                        )
                    else:
                        if not _is_regular_market_open_now():
                            log(
                                "info",
                                "tm_sl_skip_market_closed_option",
                                id=row_id,
                                symbol=symbol,
                                occ=occ,
                                qty=qty,
                            )
                            # Don't force error; try again on next loop.
                            continue

                        log(
                            "debug",
                            "tm_sl_place_option",
                            id=row_id,
                            occ=occ,
                            qty=qty,
                        )
                        fill_price, new_order_id, error_code, error_message = (
                            alpaca_client.place_option_market(
                                occ, qty, "sell_to_close"
                            )
                        )


                    # ---------- CATEGORY 1: SL order_id present ----------
                    if new_order_id and not order_id:
                        try:
                            sb = supabase_client.get_client()
                            sb.table("active_trades").update(
                                {
                                    "order_id": new_order_id,
                                    "order_status": "pending_new",
                                    "comment": "sl",
                                }
                            ).eq("id", row_id).execute()
                            order_id = new_order_id
                            order_status = "pending_new"
                            order_comment = "sl"
                        except Exception as e:
                            log(
                                "error",
                                "tm_sl_order_meta_update_error",
                                id=row_id,
                                error=str(e),
                            )

                    # ---------- CATEGORY 2 & 3: no order_id for SL ----------
                    if not new_order_id:
                        fatal_codes = {400, 401, 403, 422}
                        soft_codes = {429}

                        is_fatal = (
                            error_code in fatal_codes
                            or error_code is None
                        )
                        is_soft = (error_code in soft_codes) or (
                            isinstance(error_code, int) and error_code >= 500
                        )

                        if is_fatal:
                            # SL fatal error -> hard stop this trade (no more automation)
                            log(
                                "error",
                                "tm_sl_fatal_error",
                                id=row_id,
                                symbol=symbol,
                                http_code=error_code,
                                error=error_message,
                            )
                            try:
                                sb = supabase_client.get_client()
                                safe_msg = (error_message or "")[:150]
                                sb.table("active_trades").update(
                                    {
                                        "order_id": "Error",
                                        "order_status": "error",
                                        "manage": "N",
                                        "comment": f"sl_error_{error_code}: {safe_msg}",                        
                                    }
                                ).eq("id", row_id).execute()
                            except Exception as e:
                                log(
                                    "error",
                                    "tm_sl_fatal_error_update_failed",
                                    id=row_id,
                                    error=str(e),
                                )
                            continue

                        if is_soft:
                            # Soft error on SL -> log, keep manage='Y' and try again next loop
                            log(
                                "error",
                                "tm_sl_soft_error",
                                id=row_id,
                                symbol=symbol,
                                http_code=error_code,
                                error=error_message,
                            )
                            continue

                        # Unknown no-order_id error -> treat as fatal
                        log(
                            "error",
                            "tm_sl_unknown_error_no_order_id",
                            id=row_id,
                            symbol=symbol,
                            http_code=error_code,
                            error=error_message,
                        )
                        try:
                            sb = supabase_client.get_client()
                            sb.table("active_trades").update(
                                {
                                    "order_id": "Error",
                                    "order_status": "error",
                                    "manage": "N",
                                    "comment": "sl_error_unknown",
                                }
                            ).eq("id", row_id).execute()
                        except Exception as e:
                            log(
                                "error",
                                "tm_sl_unknown_error_update_failed",
                                id=row_id,
                                error=str(e),
                            )
                        continue

                    log(
                        "debug",
                        "tm_sl_result",
                        id=row_id,
                        symbol=symbol,
                        occ=occ,
                        sl_price_signal=sl_price_signal,
                        fill_price=fill_price,
                    )

                    # Only close if we have a confirmed fill
                    if fill_price is None:
                        log(
                            "error",
                            "tm_sl_no_fill",
                            id=row_id,
                            symbol=symbol,
                            occ=occ,
                            asset_type=asset_type,
                            qty=qty,
                        )
                    else:
                        close_price = fill_price
                        try:
                            supabase_client.update_executed_trade_close(
                                active_trade_id=row_id,
                                asset_type=asset_type,
                                qty=qty,
                                close_price=close_price,
                                reason="sl",
                            )
                        except Exception as e:
                            log(
                                "error",
                                "tm_sl_executed_update_error",
                                id=row_id,
                                error=str(e),
                            )

                        try:
                            supabase_client.delete_trade(row_id)
                        except Exception as e:
                            log(
                                "error",
                                "tm_sl_delete_error",
                                id=row_id,
                                error=str(e),
                            )

                    continue  # done with this trade


                # ---- THEN TP ----
                tp_hit, tp_price_signal = check_tp(row, spot_under, spot_option)
                log(
                    "debug",
                    "tm_tp_check",
                    id=row_id,
                    symbol=symbol,
                    tp_hit=tp_hit,
                    tp_price_signal=tp_price_signal,
                )

                if tp_hit and tp_price_signal is not None:
                    log(
                        "info",
                        "tm_tp_hit",
                        id=row_id,
                        symbol=symbol,
                        price=tp_price_signal,
                    )

                    # If a TP order is already working, don't send another
                    if order_id and order_status not in terminal_order_statuses:
                        log(
                            "debug",
                            "tm_tp_order_pending",
                            id=row_id,
                            symbol=symbol,
                            order_id=order_id,
                            order_status=order_status,
                        )
                        continue

                    new_order_id: Optional[str] = None
                    error_code: Optional[int] = None
                    error_message: Optional[str] = None

                    if asset_type == "equity":
                        log(
                            "debug",
                            "tm_tp_place_equity",
                            id=row_id,
                            symbol=symbol,
                            qty=qty,
                        )
                        fill_price, new_order_id, error_code, error_message = (
                            alpaca_client.place_equity_market(symbol, qty, "sell")
                        )
                    else:
                        if not _is_regular_market_open_now():
                            log(
                                "info",
                                "tm_tp_skip_market_closed_option",
                                id=row_id,
                                symbol=symbol,
                                occ=occ,
                                qty=qty,
                            )
                            continue

                        log(
                            "debug",
                            "tm_tp_place_option",
                            id=row_id,
                            occ=occ,
                            qty=qty,
                        )
                        fill_price, new_order_id, error_code, error_message = (
                            alpaca_client.place_option_market(
                                occ, qty, "sell_to_close"
                            )
                        )


                    # ---------- CATEGORY 1: TP order_id present ----------
                    if new_order_id and not order_id:
                        try:
                            sb = supabase_client.get_client()
                            sb.table("active_trades").update(
                                {
                                    "order_id": new_order_id,
                                    "order_status": "pending_new",
                                    "comment": "tp",
                                }
                            ).eq("id", row_id).execute()
                            order_id = new_order_id
                            order_status = "pending_new"
                            order_comment = "tp"
                        except Exception as e:
                            log(
                                "error",
                                "tm_tp_order_meta_update_error",
                                id=row_id,
                                error=str(e),
                            )

                    # ---------- CATEGORY 2 & 3: no order_id for TP ----------
                    if not new_order_id:
                        fatal_codes = {400, 401, 403, 422}
                        soft_codes = {429}

                        is_fatal = (
                            error_code in fatal_codes
                            or error_code is None
                        )
                        is_soft = (error_code in soft_codes) or (
                            isinstance(error_code, int) and error_code >= 500
                        )

                        if is_fatal:
                            log(
                                "error",
                                "tm_tp_fatal_error",
                                id=row_id,
                                symbol=symbol,
                                http_code=error_code,
                                error=error_message,
                            )
                            try:
                                sb = supabase_client.get_client()
                                safe_msg = (error_message or "")[:150]
                                sb.table("active_trades").update(
                                    {
                                        "order_id": "Error",
                                        "order_status": "error",
                                        "manage": "N",
                                        "comment": f"tp_error_{error_code}: {safe_msg}",                                       
                                    }
                                ).eq("id", row_id).execute()
                            except Exception as e:
                                log(
                                    "error",
                                    "tm_tp_fatal_error_update_failed",
                                    id=row_id,
                                    error=str(e),
                                )
                            continue

                        if is_soft:
                            log(
                                "error",
                                "tm_tp_soft_error",
                                id=row_id,
                                symbol=symbol,
                                http_code=error_code,
                                error=error_message,
                            )
                            continue

                        log(
                            "error",
                            "tm_tp_unknown_error_no_order_id",
                            id=row_id,
                            symbol=symbol,
                            http_code=error_code,
                            error=error_message,
                        )
                        try:
                            sb = supabase_client.get_client()
                            sb.table("active_trades").update(
                                {
                                    "order_id": "Error",
                                    "order_status": "error",
                                    "manage": "N",
                                    "comment": "tp_error_unknown",
                                }
                            ).eq("id", row_id).execute()
                        except Exception as e:
                            log(
                                "error",
                                "tm_tp_unknown_error_update_failed",
                                id=row_id,
                                error=str(e),
                            )
                        continue

                    log(
                        "debug",
                        "tm_tp_result",
                        id=row_id,
                        symbol=symbol,
                        occ=occ,
                        tp_price_signal=tp_price_signal,
                        fill_price=fill_price,
                    )

                    # Only close if we have a confirmed fill
                    if fill_price is None:
                        log(
                            "error",
                            "tm_tp_no_fill",
                            id=row_id,
                            symbol=symbol,
                            occ=occ,
                            asset_type=asset_type,
                            qty=qty,
                        )
                    else:
                        close_price = fill_price
                        try:
                            supabase_client.update_executed_trade_close(
                                active_trade_id=row_id,
                                asset_type=asset_type,
                                qty=qty,
                                close_price=close_price,
                                reason="tp",
                            )
                        except Exception as e:
                            log(
                                "error",
                                "tm_tp_executed_update_error",
                                id=row_id,
                                error=str(e),
                            )

                        try:
                            supabase_client.delete_trade(row_id)
                        except Exception as e:
                            log(
                                "error",
                                "tm_tp_delete_error",
                                id=row_id,
                                error=str(e),
                            )

                    continue  # done with this trade


        time_module.sleep(settings.trade_manager_interval)
