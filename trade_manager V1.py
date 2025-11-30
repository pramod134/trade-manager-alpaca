import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

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
    """

    cond = (row.get("entry_cond") or "").lower()
    level = row.get("entry_level")
    entry_type = (row.get("entry_type") or "equity").lower()
    entry_tf = row.get("entry_tf")

    # Pick which instrument we’re using for entry (equity vs option)
    spot_row = _choose_spot_row(row, entry_type, spot_under, spot_option)
    if not spot_row:
        return False, None

    # -------- NOW: tick-based entry on spot price --------
    if cond == "now":
        price = _get_spot_price(spot_row)
        # If we have a price, we always return it for logging, even if we don't enter
        if price is None:
            return False, None
        # "now" is unconditional: if manage='Y' and status='nt-waiting', this fires immediately
        return True, price

    # -------- CA / CB: candle-based entry on TF close --------
    if cond in ("ca", "cb"):
        if entry_tf is None:
            return False, None

        price = _get_tf_close(spot_row, entry_tf)

        # Always return the price used for the decision (if any), so logs can see it
        if price is None or level is None:
            return False, price

        is_long = row.get("side") == "long"

        # ca = close ABOVE the level
        if cond == "ca":
            should_enter = price > level
            return should_enter, price

        # cb = close BELOW the level
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
            time.sleep(settings.trade_manager_interval)
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
            )

            # Fetch spot rows for underlying + option
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

            log(
                "debug",
                "tm_spot_context",
                id=row_id,
                symbol=symbol,
                under_last=_get_spot_price(spot_under),
                option_last=_get_spot_price(spot_option),
            )

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
                        fill_price = alpaca_client.place_equity_market(
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
                        fill_price = alpaca_client.place_option_market(
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

                    # PATCH: Only treat as closed if we have a confirmed fill price.
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

            # ---------- STATUS = 'nt-waiting' (entry) ----------
            if status == "nt-waiting":
                should_enter, entry_price = check_entry(
                    row, spot_under, spot_option
                )
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

                # Place order (asset_type still controls what we BUY/SELL, which is correct)
                if asset_type == "equity":
                    log(
                        "debug",
                        "tm_entry_place_equity",
                        id=row_id,
                        symbol=symbol,
                        qty=qty,
                    )
                    fill_price = alpaca_client.place_equity_market(
                        symbol, qty, "buy"
                    )
                else:
                    log(
                        "debug",
                        "tm_entry_place_option",
                        id=row_id,
                        occ=occ,
                        qty=qty,
                    )
                    fill_price = alpaca_client.place_option_market(
                        occ, qty, "buy_to_open"
                    )

                log(
                    "debug",
                    "tm_entry_result",
                    id=row_id,
                    symbol=symbol,
                    occ=occ,
                    entry_price=entry_price,
                    fill_price=fill_price,
                )

                # PATCH: Only move to managing if we have a confirmed fill.
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
                    continue

                open_price = fill_price

                try:
                    supabase_client.insert_executed_trade_open(row, open_price)
                    supabase_client.mark_as_managing(row_id)
                    log(
                        "info",
                        "tm_entry_db_update",
                        id=row_id,
                        symbol=symbol,
                        open_price=open_price,
                    )
                except Exception as e:
                    log(
                        "error",
                        "tm_entry_db_error",
                        id=row_id,
                        error=str(e),
                    )

                continue

            # ---------- STATUS = 'nt-managing' / 'pos-managing' (SL / TP) ----------
            if status in ("nt-managing", "pos-managing"):
                # Check SL first
                sl_hit, sl_price_signal = check_sl(
                    row, spot_under, spot_option
                )
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

                    if asset_type == "equity":
                        log(
                            "debug",
                            "tm_sl_place_equity",
                            id=row_id,
                            symbol=symbol,
                            qty=qty,
                        )
                        fill_price = alpaca_client.place_equity_market(
                            symbol, qty, "sell"
                        )
                    else:
                        log(
                            "debug",
                            "tm_sl_place_option",
                            id=row_id,
                            occ=occ,
                            qty=qty,
                        )
                        fill_price = alpaca_client.place_option_market(
                            occ, qty, "sell_to_close"
                        )

                    log(
                        "debug",
                        "tm_sl_result",
                        id=row_id,
                        symbol=symbol,
                        occ=occ,
                        sl_price_signal=sl_price_signal,
                        fill_price=fill_price,
                    )

                    # PATCH: Only close if we have a confirmed fill.
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

                # Then TP
                tp_hit, tp_price_signal = check_tp(
                    row, spot_under, spot_option
                )
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

                    if asset_type == "equity":
                        log(
                            "debug",
                            "tm_tp_place_equity",
                            id=row_id,
                            symbol=symbol,
                            qty=qty,
                        )
                        fill_price = alpaca_client.place_equity_market(
                            symbol, qty, "sell"
                        )
                    else:
                        log(
                            "debug",
                            "tm_tp_place_option",
                            id=row_id,
                            occ=occ,
                            qty=qty,
                        )
                        fill_price = alpaca_client.place_option_market(
                            occ, qty, "sell_to_close"
                        )

                    log(
                        "debug",
                        "tm_tp_result",
                        id=row_id,
                        symbol=symbol,
                        occ=occ,
                        tp_price_signal=tp_price_signal,
                        fill_price=fill_price,
                    )

                    # PATCH: Only close if we have a confirmed fill.
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

        time.sleep(settings.trade_manager_interval)
