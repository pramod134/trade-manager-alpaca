import time as time_module
from datetime import datetime, timezone, time
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo


from config import settings
from logger import log
import supabase_client
import alpaca_client

# ================================================================
# TRADE MANAGER CACHE (Spec v2)
# ================================================================

# Cache structure:
#   cache[id] = {
#       "row": <snapshot of active_trades row>,
#       "mode": "entry" | "exit" | "force_close" | "recovery",
#       "attempts": 0,
#       "order_id": None or str,
#       "reason": None or "sl" or "tp" or "force_close",
#   }
#
# IMPORTANT:
#   - This cache *temporarily* tracks trades that need broker action.
#   - Supabase remains the source of truth.
#   - On restart, cache is auto-repopulated by scanning Supabase rows
#     that already have order_id != NULL.

cache: dict[str, dict] = {}

# Alpaca terminal order states
TERMINAL_STATES = {"filled", "canceled", "rejected", "expired"}


def cache_add(row: dict, mode: str, reason: str | None = None) -> None:
    """
    Insert a row into the cache if not already present.
    Must include snapshot of row + execution metadata.
    """
    row_id = row["id"]
    if row_id in cache:
        return

    cache[row_id] = {
        "row": row.copy(),
        "mode": mode,           # "entry" / "exit" / "force_close" / "recovery"
        "attempts": 0,
        "order_id": row.get("order_id"),
        "reason": reason,       # "sl" / "tp" / "force_close" or None
    }


def cache_remove(row_id: str) -> None:
    """Remove a row from cache safely."""
    if row_id in cache:
        del cache[row_id]


def is_terminal_status(status: str | None) -> bool:
    """Check if Alpaca order status is terminal."""
    if not status:
        return False
    return status.lower() in TERMINAL_STATES


def should_block_option_order_now(asset_type: str) -> bool:
    """
    Return True if current time is OUTSIDE permitted RTH window
    for options, meaning we must NOT send the order yet.

    Note:
        This does NOT count as an "attempt".
        The cache item simply waits until RTH opens.
    """
    if asset_type != "option":
        return False
    from datetime import datetime, time
    from zoneinfo import ZoneInfo

    now_et = datetime.now(ZoneInfo("America/New_York"))
    if now_et.weekday() >= 5:
        return True  # weekend

    t = now_et.time()
    # Spec window: 09:31–15:59
    if t < time(9, 31) or t > time(15, 59):
        return True

    return False


def _initialize_cache_from_supabase_on_start() -> None:
    """
    On process start, rebuild the cache for any active_trades rows that
    already have a non-null Alpaca order_id.

    This supports crash/restart behavior:
      - We NEVER send a second order for these rows.
      - We only poll Alpaca and reconcile based on the existing order_id.
    """
    try:
        rows = supabase_client.fetch_active_trades()
    except Exception as e:
        log("error", "tm_cache_init_fetch_error", error=str(e))
        return

    count = 0
    for row in rows:
        row_id = row.get("id")
        if not row_id:
            continue

        order_id = (row.get("order_id") or "").strip()
        order_status = (row.get("order_status") or "").lower()
        manage = row.get("manage")
        status = row.get("status")

        # If there's no real order_id, nothing to recover
        if not order_id:
            continue

        # Ignore legacy sentinel values like "error" / "Error" that are not real order_ids
        if order_id.lower() == "error":
            continue

        # If the row is already marked as error and not force-close, skip
        if order_status == "error" and manage != "C":
            continue

        # Add to cache in recovery mode; we will ONLY poll Alpaca for this order,
        # never send a new order for this id.
        cache_add(row, mode="recovery")

        log(
            "info",
            "tm_cache_init_recovery_add",
            id=row_id,
            order_id=order_id,
            status=status,
            manage=manage,
            order_status=order_status,
        )
        count += 1

    log("info", "tm_cache_init_complete", recovered=count)





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




def _scan_supabase_for_new_work() -> None:
    """
    Scan active_trades in Supabase and enqueue work into the in-memory cache.

    Rules (spec v2):

      - Skip any row whose id is already present in _CACHE.
      - If manage = 'C'  -> enqueue as mode='force_close'.
      - Else if manage != 'Y' -> skip.
      - Else if status = 'pos-managing' -> skip.
      - Else if status = 'nt-waiting' & manage='Y' -> evaluate ENTRY, enqueue mode='entry' if triggered.
      - Else if status = 'nt-managing' & manage='Y' -> evaluate SL/TP, enqueue mode='exit' if triggered.

    Crash / restart behavior:

      - If order_id is NOT NULL (and not the legacy sentinel 'Error'):
            → NEVER send a new broker order for that row.
            → Enqueue as mode='recovery' so the cache driver can poll Alpaca
              and finalize based on the real broker status.
    """

    from typing import cast

    global _CACHE  # defined in earlier patches as Dict[str, CacheEntry]

    try:
        active_rows = supabase_client.fetch_active_trades()
    except Exception as e:
        log(
            "error",
            "tm_scan_fetch_active_trades_error",
            error=str(e),
        )
        return

    if not active_rows:
        return

    for row in active_rows:
        # Defensive: rows are plain dicts from Supabase client
        row = cast(dict, row)
        row_id = row.get("id")
        if not row_id:
            continue

        if row_id in _CACHE:
            # Already being processed in cache
            log(
                "debug",
                "tm_scan_skip_already_in_cache",
                id=row_id,
            )
            continue

        manage = (row.get("manage") or "").upper()
        status = (row.get("status") or "").lower()
        asset_type = (row.get("asset_type") or "").lower()
        symbol = row.get("symbol")
        occ = row.get("occ")
        qty = row.get("qty")

        order_id = row.get("order_id")
        order_status = (row.get("order_status") or "").lower()

        log(
            "debug",
            "tm_scan_row_context",
            id=row_id,
            symbol=symbol,
            occ=occ,
            manage=manage,
            status=status,
            asset_type=asset_type,
            qty=qty,
            order_id=order_id,
            order_status=order_status,
        )

        # ------------------------------------------------------------------
        # 1) Crash / restart recovery: existing broker order_id => attach to cache
        # ------------------------------------------------------------------
        #
        # If an order_id already exists (and it's not the legacy sentinel "Error"),
        # we MUST NOT send a second order. Instead, attach this row to cache
        # as a recovery entry. The cache driver will:
        #   - Poll Alpaca /v2/orders/{order_id}
        #   - Finalize based on terminal / non-terminal broker status
        #
        if order_id and str(order_id).lower() != "error":
            try:
                entry = CacheEntry(
                    id=row_id,
                    row=row,
                    mode="recovery",
                    attempts=0,
                    order_id=str(order_id),
                    exit_reason=None,
                )
            except Exception:
                # Fallback if CacheEntry signature ever changes: simple attribute set
                entry = CacheEntry(id=row_id, row=row, mode="recovery")
                entry.attempts = 0
                entry.order_id = str(order_id)
                entry.exit_reason = None

            _CACHE[row_id] = entry

            log(
                "info",
                "tm_scan_attach_recovery",
                id=row_id,
                symbol=symbol,
                occ=occ,
                order_id=order_id,
                status=status,
                manage=manage,
            )
            # Once we know a broker order exists, we never enqueue fresh entry/exit
            # work for this row until the cache finishes recovery.
            continue

        # ------------------------------------------------------------------
        # 2) manage = 'C'  → FORCE CLOSE
        # ------------------------------------------------------------------
        if manage == "C":
            try:
                entry = CacheEntry(
                    id=row_id,
                    row=row,
                    mode="force_close",
                    attempts=0,
                    order_id=None,
                    exit_reason="force_close",
                )
            except Exception:
                entry = CacheEntry(id=row_id, row=row, mode="force_close")
                entry.attempts = 0
                entry.order_id = None
                entry.exit_reason = "force_close"

            _CACHE[row_id] = entry

            log(
                "info",
                "tm_scan_enqueue_force_close",
                id=row_id,
                symbol=symbol,
                occ=occ,
                status=status,
                manage=manage,
            )
            continue

        # ------------------------------------------------------------------
        # 3) manage != 'Y' → not managed, ignore
        # ------------------------------------------------------------------
        if manage != "Y":
            log(
                "debug",
                "tm_scan_skip_not_managed",
                id=row_id,
                manage=manage,
            )
            continue

        # ------------------------------------------------------------------
        # 4) status = 'pos-managing' → ignored by manager per spec
        # ------------------------------------------------------------------
        if status == "pos-managing":
            log(
                "debug",
                "tm_scan_skip_pos_managing",
                id=row_id,
                status=status,
            )
            continue

        # ------------------------------------------------------------------
        # 5) Fetch spot rows once per row before entry/SL/TP checks
        # ------------------------------------------------------------------
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
                "tm_scan_fetch_spot_error",
                id=row_id,
                symbol=symbol,
                occ=occ,
                error=str(e),
            )
            # Can't evaluate conditions without spot data
            continue

        # ------------------------------------------------------------------
        # 6) status = 'nt-waiting'  → ENTRY evaluation
        # ------------------------------------------------------------------
        if status == "nt-waiting":
            should_enter, entry_price = check_entry(row, spot_under, spot_option)

            log(
                "debug",
                "tm_scan_entry_check",
                id=row_id,
                symbol=symbol,
                should_enter=should_enter,
                entry_price=entry_price,
            )

            if not should_enter or entry_price is None:
                continue

            try:
                entry = CacheEntry(
                    id=row_id,
                    row=row,
                    mode="entry",
                    attempts=0,
                    order_id=None,
                    exit_reason=None,
                )
            except Exception:
                entry = CacheEntry(id=row_id, row=row, mode="entry")
                entry.attempts = 0
                entry.order_id = None
                entry.exit_reason = None

            _CACHE[row_id] = entry

            log(
                "info",
                "tm_scan_enqueue_entry",
                id=row_id,
                symbol=symbol,
                price=entry_price,
            )
            continue

        # ------------------------------------------------------------------
        # 7) status = 'nt-managing' → SL / TP evaluation
        # ------------------------------------------------------------------
        if status == "nt-managing":
            sl_hit, sl_price = check_sl(row, spot_under, spot_option)
            tp_hit, tp_price = check_tp(row, spot_under, spot_option)

            log(
                "debug",
                "tm_scan_sl_tp_check",
                id=row_id,
                symbol=symbol,
                sl_hit=sl_hit,
                sl_price=sl_price,
                tp_hit=tp_hit,
                tp_price=tp_price,
            )

            if not sl_hit and not tp_hit:
                continue

            # If both fire in same loop, prefer SL (more defensive),
            # which matches typical risk-first behavior.
            if sl_hit:
                reason = "sl"
            else:
                reason = "tp"

            try:
                entry = CacheEntry(
                    id=row_id,
                    row=row,
                    mode="exit",
                    attempts=0,
                    order_id=None,
                    exit_reason=reason,
                )
            except Exception:
                entry = CacheEntry(id=row_id, row=row, mode="exit")
                entry.attempts = 0
                entry.order_id = None
                entry.exit_reason = reason

            _CACHE[row_id] = entry

            log(
                "info",
                "tm_scan_enqueue_exit",
                id=row_id,
                symbol=symbol,
                reason=reason,
            )
            continue

        # ------------------------------------------------------------------
        # 8) Any other status is currently ignored
        # ------------------------------------------------------------------
        log(
            "debug",
            "tm_scan_skip_status",
            id=row_id,
            status=status,
        )



# ---------- MAIN LOOP (Spec v2) ----------


def run_trade_manager() -> None:
    """
    Main manager loop (Spec v2):

      1) On startup, rebuild cache for any rows that already have an order_id
         so we NEVER send a second order for the same active_trades.id.

      2) Each loop:
           - process in-cache items (send first order or poll existing order_id)
           - scan Supabase for new work (entry / exit / force_close)
           - sleep a short interval
    """
    log("info", "trade_manager_start", interval=settings.trade_manager_interval)

    # Crash/restart recovery: populate cache from any rows that already have order_id
    _initialize_cache_from_supabase_on_start()

    while True:
        # 1) Process all cache entries (entry / exit / force_close / recovery)
        try:
            _process_cache_once()
        except Exception as e:
            log("error", "tm_process_cache_error", error=str(e))

        # 2) Scan Supabase for new tasks (only rows NOT already in cache)
        try:
            _scan_supabase_for_new_work()
        except Exception as e:
            log("error", "tm_scan_supabase_error", error=str(e))

        # 3) Throttle loop
        time_module.sleep(settings.trade_manager_interval)


def _process_cache_once() -> None:
    """
    Iterate over cache[id] entries and:

      - If order_id is None:
            send the appropriate Alpaca order (entry/exit/force_close),
            with RTH checks for options and retry accounting.

      - If order_id is not None:
            poll Alpaca for updated status and finalize when terminal
            (filled / canceled / rejected / expired) according to Spec v2.

    NOTE:
      Full behavior (send + poll + finalize) will be implemented in later
      patches; this stub is here so the new main loop is structurally valid.
    """
    # This will be fully implemented in subsequent patches.
    # For now, just iterate and log so the function is not empty.
    if not cache:
        return

    for row_id, entry in list(cache.items()):
        mode = entry.get("mode")
        order_id = entry.get("order_id")
        log(
            "debug",
            "tm_cache_stub_entry",
            id=row_id,
            mode=mode,
            order_id=order_id,
        )
        # Real send/poll/finalize logic comes in later patches.


def _scan_supabase_for_new_work() -> None:
    """
    Scan active_trades and decide which rows should enter cache:

      - If id already in cache -> skip
      - If manage = 'C'        -> cache_add(..., mode='force_close')
      - If manage != 'Y'       -> skip
      - If status = 'pos-managing' -> skip (ignored by manager)
      - If status = 'nt-waiting' & manage='Y':
           evaluate ENTRY conditions (using existing check_entry)
           and, if triggered, cache_add(..., mode='entry')
      - If status = 'nt-managing' & manage='Y':
           evaluate SL/TP (using existing check_sl/check_tp)
           and, if triggered, cache_add(..., mode='exit', reason='sl'/'tp')

    NOTE:
      This function’s detailed behavior (including calling check_entry,
      check_sl, check_tp, and respecting all your existing direction logic)
      will be implemented in later patches.
    """
    rows = supabase_client.fetch_active_trades()
    log("debug", "tm_scan_stub_rows", count=len(rows))

    # Detailed selection logic (entry/sl/tp/force_close) is coming
    # in subsequent patches; here we only keep a stub so the loop runs.

