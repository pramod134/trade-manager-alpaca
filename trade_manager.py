# ================================
# trade_manager.py  (REWRITE V1)
# PART 1 / 3
# ================================

import time
import uuid
from datetime import datetime, timezone

from supabase import create_client, Client
from alpaca_client import place_equity_market, place_option_market
from logger import log
from config import settings


# ================================================================
#  GLOBAL SETTINGS & SUPABASE CLIENT
# ================================================================

# Use settings object from config
SUPABASE_URL = settings.supabase_url
SUPABASE_KEY = settings.supabase_key

ALPACA_BASE = settings.alpaca_base
ALPACA_KEY = settings.alpaca_key
ALPACA_SECRET = settings.alpaca_secret


sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ================================================================
#  HELPER: CALL ALPACA REST ORDER API FOR STATUS
# ================================================================

import requests

def get_alpaca_order(order_id: str):
    """
    Calls Alpaca REST API to fetch order status for a given order_id.
    Returns JSON dict with fields including "status".
    Raises exception on HTTP error.
    """
    url = f"{ALPACA_BASE}/v2/orders/{order_id}"
    headers = {
        "APCA-API-KEY-ID": config.ALPACA_KEY,
        "APCA-API-SECRET-KEY": config.ALPACA_SECRET,
    }
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise Exception(f"Alpaca order status error: {r.status_code} - {r.text}")
    return r.json()


# ================================================================
#  HELPER: IS TERMINAL STATUS?
# ================================================================

TERMINAL_STATUSES = {"filled", "canceled", "expired", "rejected"}

def is_terminal(status: str) -> bool:
    """
    Alpaca order status considered terminal.
    """
    return status.lower() in TERMINAL_STATUSES


# ================================================================
#  CACHE OBJECT
# ================================================================

class TradeCacheEntry:
    """
    Represents an in-progress trade transaction.
    This object lives only while an entry/exit/force-close is ongoing.
    After terminal status is reached (success or error), it leaves cache.
    """

    def __init__(self, row: dict):
        # full snapshot of the active_trades row
        self.row = row.copy()

        # State machine tags
        self.mode = None  # "entry", "exit", or "force_close"

        # Alpaca order IDs
        self.entry_order_id = None
        self.exit_order_id = None

        # attempt counters
        self.attempts = 0

        # timestamp tracking
        self.started_at = datetime.now(timezone.utc)

    def __repr__(self):
        return f"<TradeCacheEntry id={self.row.get('id')} mode={self.mode} attempts={self.attempts}>"



# ================================================================
# DATABASE HELPERS
# ================================================================

def db_update_active_trade(id: str, fields: dict):
    """
    Update active_trades row for given id with provided dict fields.
    """
    sb.table("active_trades").update(fields).eq("id", id).execute()


def db_delete_active_trade(id: str):
    """
    Delete active_trades row for the given id.
    """
    sb.table("active_trades").delete().eq("id", id).execute()


def db_insert_executed(trade: dict):
    """
    Insert a completed trade snapshot into executed_trades.
    Expecting:
      {
        "active_trade_id": ...,
        "trade_type": "entry"/"exit"/"sl"/"tp"/"force_close"/"error_entry"/"error_exit",
        "symbol": ...,
        "occ": ...,
        "asset_type": ...,
        "qty": ...,
        "open_ts": ...,
        "open_price": ...,
        "open_cost_basis": ...,
        "close_ts": ...,
        "close_price": ...,
        "close_cost_basis": ...,
        "close_reason": ...,
      }
    Missing fields default to NULL as defined in schema.
    """
    sb.table("executed_trades").insert(trade).execute()



# ================================================================
# PRICE ACCESS (SPOT)
# ================================================================

def get_spot(symbol: str) -> float:
    """
    Reads the most recent last_close from active_trades OR external spot source
    as per your system design.

    For now, using active_trades.last_close for 'symbol' rows.
    Modify as needed to use your actual spot-updater logic.
    """
    res = (
        sb.table("active_trades")
        .select("last_close")
        .eq("symbol", symbol)
        .order("updated_at", desc=True)
        .limit(1)
        .execute()
    )

    if not res.data:
        raise Exception(f"No spot/last_close for symbol {symbol}")

    return float(res.data[0]["last_close"] or 0.0)


# ================================================================
# MANAGER MAIN CLASS (PART 1 END)
# ================================================================

class TradeManager:
    """
    Main trade manager orchestrator.

    Responsibilities:
      - Scan active_trades for:
            • manage="C" → force close
            • nt-waiting  + manage="Y" → entry logic
            • nt-managing + manage="Y" → SL/TP logic
      - Keep a cache of in-flight trades
      - Submit broker orders with retry logic
      - Poll Alpaca for order status until terminal
      - Update Supabase at minimal points
      - Log executed trades and delete rows after close
    """

    def __init__(self):
        self.cache = {}   # id -> TradeCacheEntry

    # PART 2 will contain the actual flows
    # PART 3 will contain the run() loop

# ================================
# trade_manager.py  (REWRITE V1)
# PART 2 / 3
# ================================

    # ================================================================
    #  INTERNAL HELPERS — ORDER SUBMISSION
    # ================================================================

    def _place_order(self, entry: TradeCacheEntry, side: str, is_entry: bool):
        """
        Place an Alpaca order depending on asset_type.
        Returns tuple: (fill_price, order_id, error_code, error_msg).
        We DO NOT expect immediate fill; fill_price may be None/0.
        """
        row = entry.row
        asset_type = row["asset_type"]
        qty = int(row["qty"])

        if asset_type == "equity":
            symbol = row["symbol"]
            return place_equity_market(symbol, qty, side)

        elif asset_type == "option":
            occ = row["occ"]
            return place_option_market(occ, qty, side)

        else:
            return (0.0, None, "invalid_asset_type", f"Unsupported asset_type {asset_type}")


    # ================================================================
    #  INTERNAL HELPERS — POLL ORDER STATUS
    # ================================================================

    def _poll_order(self, order_id: str):
        """
        Poll Alpaca REST until order_id evolves.
        Returns JSON dict with Alpaca status fields.
        """
        try:
            return get_alpaca_order(order_id)
        except Exception as e:
            # Not terminal; treat as transient error and let main loop retry
            log("error", "alpaca_poll_error", order_id=order_id, error=str(e))
            return None


    # ================================================================
    #  INTERNAL HELPER — RECORD EXECUTION (ENTRY or EXIT)
    # ================================================================

    def _log_execution(self, entry: TradeCacheEntry, trade_type: str,
                       open_ts=None, open_price=None, open_cost_basis=None,
                       close_ts=None, close_price=None, close_cost_basis=None, close_reason=None):

        # Convert datetimes to ISO strings so Supabase JSON encoder doesn’t choke
        def _ts(val):
            if isinstance(val, datetime):
                return val.isoformat()
            return val

        row = entry.row
        executed = {
            "active_trade_id": row["id"],
            "trade_type": trade_type,
            "symbol": row["symbol"],
            "occ": row["occ"],
            "asset_type": row["asset_type"],
            "qty": row["qty"],
            "open_ts": _ts(open_ts),
            "open_price": open_price,
            "open_cost_basis": open_cost_basis,
            "close_ts": _ts(close_ts),
            "close_price": close_price,
            "close_cost_basis": close_cost_basis,
            "close_reason": close_reason,
        }

        db_insert_executed(executed)


    # ================================================================
    #  INTERNAL — HANDLE RETRY-FAIL
    # ================================================================

    def _fail_after_retries(self, entry: TradeCacheEntry, error_msg: str):
        """
        Called when all 3 attempts failed.
        We mark manage='N', update DB with error, drop from cache.
        No row is written to executed_trades, because nothing executed.
        """
        id_ = entry.row["id"]

        entry.row["order_status"] = "error"
        entry.row["comment"] = error_msg
        entry.row["manage"] = "N"

        db_update_active_trade(id_, {
            "order_status": "error",
            "comment": error_msg,
            "manage": "N",
        })

        log(
            "error",
            "tm_fail_after_retries",
            id=id_,
            mode=entry.mode,
            error=error_msg,
        )

        if id_ in self.cache:
            del self.cache[id_]


    # ================================================================
    #  ENTRY FLOW
    # ================================================================

    def _start_entry(self, row: dict):
        """
        Triggered when entry condition is satisfied.
        Move row to cache and attempt order.
        """
        id_ = row["id"]
        log("info", "tm_entry_start", id=id_, symbol=row["symbol"])

        entry = TradeCacheEntry(row)
        entry.mode = "entry"
        self.cache[id_] = entry


    def _process_entry(self, entry: TradeCacheEntry):
        """
        Handles submission of entry order and polling for fill.
        """
        id_ = entry.row["id"]

        # ORDER NOT YET SUBMITTED
        if entry.entry_order_id is None:
            entry.attempts += 1
            side = "buy" if entry.row["trade_type"] == "long" else "sell"

            fill_price, order_id, err_code, err_msg = self._place_order(entry, side, is_entry=True)

            # retry logic on failure
            if order_id is None:
                if entry.attempts >= 3:
                    self._fail_after_retries(entry, err_msg or "entry order failed")
                return

            # success: store order_id, write minimal DB update
            entry.entry_order_id = order_id
            entry.row["order_id"] = order_id
            entry.row["order_status"] = "submitted"

            db_update_active_trade(id_, {
                "order_id": order_id,
                "order_status": "submitted",
            })

            log("info", "tm_entry_order_submitted", id=id_, order_id=order_id)
            return

        # ORDER SUBMITTED — POLL FOR TERMINAL STATUS
        status_info = self._poll_order(entry.entry_order_id)
        if not status_info:
            return

        alpaca_status = status_info.get("status", "").lower()

        # NOT TERMINAL → continue polling
        if not is_terminal(alpaca_status):
            return

        # TERMINAL: FILLED vs FAILURE
        if alpaca_status == "filled":
            fill_price = float(status_info.get("filled_avg_price") or 0)
            qty = int(status_info.get("filled_qty") or entry.row["qty"])
            now = datetime.now(timezone.utc)

            # update row for nt-managing
            entry.row["status"] = "nt-managing"
            entry.row["order_status"] = "filled"

            db_update_active_trade(id_, {
                "status": "nt-managing",
                "order_status": "filled",
            })

            # log execution
            cost_basis = fill_price * qty
            self._log_execution(entry,
                                "entry",
                                open_ts=now,
                                open_price=fill_price,
                                open_cost_basis=cost_basis)

            log("info", "tm_entry_filled", id=id_, order_id=entry.entry_order_id)
            del self.cache[id_]
            return

        # TERMINAL BUT NOT FILLED → failure path
        self._fail_after_retries(entry, f"Entry ended as {alpaca_status}")


    # ================================================================
    #  EXIT FLOW (SL / TP)
    # ================================================================

    def _start_exit(self, row: dict, reason: str):
        """
        reason = "sl" or "tp"
        """
        id_ = row["id"]
        log("info", "tm_exit_start", id=id_, symbol=row["symbol"], reason=reason)

        entry = TradeCacheEntry(row)
        entry.mode = "exit"
        entry.exit_reason = reason
        self.cache[id_] = entry


    def _process_exit(self, entry: TradeCacheEntry):
        """
        Attempts to close position, then polls for fill.
        """
        id_ = entry.row["id"]
        reason = entry.exit_reason

        # ORDER NOT YET SUBMITTED
        if entry.exit_order_id is None:
            entry.attempts += 1

            side = "sell" if entry.row["trade_type"] == "long" else "buy"

            fill_price, order_id, err_code, err_msg = self._place_order(entry, side, is_entry=False)

            if order_id is None:
                if entry.attempts >= 3:
                    self._fail_after_retries(entry, err_msg or "exit order failed")
                return

            # success
            entry.exit_order_id = order_id
            entry.row["order_id"] = order_id
            entry.row["order_status"] = "submitted"

            db_update_active_trade(id_, {
                "order_id": order_id,
                "order_status": "submitted",
            })

            log("info", "tm_exit_order_submitted", id=id_, order_id=order_id, reason=reason)
            return

        # ORDER SUBMITTED — POLL
        status_info = self._poll_order(entry.exit_order_id)
        if not status_info:
            return

        alpaca_status = status_info.get("status", "").lower()

        if not is_terminal(alpaca_status):
            return

        # FILLED → SUCCESSFUL EXIT
        if alpaca_status == "filled":
            fill_price = float(status_info.get("filled_avg_price") or 0)
            qty = int(status_info.get("filled_qty") or entry.row["qty"])
            now = datetime.now(timezone.utc)

            cost_basis = fill_price * qty

            # log before deletion
            self._log_execution(entry,
                                entry.exit_reason,
                                open_ts=None,
                                open_price=None,
                                open_cost_basis=None,
                                close_ts=now,
                                close_price=fill_price,
                                close_cost_basis=cost_basis,
                                close_reason=entry.exit_reason)

            # delete active trade
            db_delete_active_trade(id_)
            log("info", "tm_exit_filled", id=id_, order_id=entry.exit_order_id, reason=reason)

            del self.cache[id_]
            return

        # failure (cancelled / expired / rejected)
        self._fail_after_retries(entry, f"Exit ended as {alpaca_status}")


    # ================================================================
    #  FORCE-CLOSE FLOW
    # ================================================================

    def _start_force_close(self, row: dict):
        id_ = row["id"]
        log("warning", "tm_force_close_start", id=id_, symbol=row["symbol"])

        entry = TradeCacheEntry(row)
        entry.mode = "force_close"
        self.cache[id_] = entry


    def _process_force_close(self, entry: TradeCacheEntry):
        """
        Force close is same as exit but reason always 'force_close'
        """
        id_ = entry.row["id"]

        # ORDER NOT SUBMITTED
        if entry.exit_order_id is None:
            entry.attempts += 1

            side = "sell" if entry.row["trade_type"] == "long" else "buy"

            fill_price, order_id, err_code, err_msg = self._place_order(entry, side, is_entry=False)
            if order_id is None:
                if entry.attempts >= 3:
                    self._fail_after_retries(entry, err_msg or "force-close failed")
                return

            entry.exit_order_id = order_id
            entry.row["order_id"] = order_id
            entry.row["order_status"] = "submitted"

            db_update_active_trade(id_, {
                "order_id": order_id,
                "order_status": "submitted",
            })

            log("warning", "tm_force_close_order_submitted", id=id_, order_id=order_id)
            return

        # POLL
        status_info = self._poll_order(entry.exit_order_id)
        if not status_info:
            return

        alpaca_status = status_info.get("status", "")

        if not is_terminal(alpaca_status):
            return

        # SUCCESS
        if alpaca_status == "filled":
            fill_price = float(status_info.get("filled_avg_price") or 0)
            qty = int(status_info.get("filled_qty") or entry.row["qty"])
            now = datetime.now(timezone.utc)

            cost_basis = fill_price * qty

            # log
            self._log_execution(entry,
                                "force_close",
                                open_ts=None,
                                open_price=None,
                                open_cost_basis=None,
                                close_ts=now,
                                close_price=fill_price,
                                close_cost_basis=cost_basis,
                                close_reason="force_close")

            # delete row
            db_delete_active_trade(id_)
            log("warning", "tm_force_close_filled", id=id_, order_id=entry.exit_order_id)
            del self.cache[id_]
            return

        # FAILURE
        self._fail_after_retries(entry, f"force-close ended as {alpaca_status}")



# ================================
# trade_manager.py  (REWRITE V1)
# PART 3 / 3
# ================================

    # ================================================================
    # CONDITION CHECKS
    # ================================================================

    def _check_entry_condition(self, row: dict) -> bool:
        """
        Evaluates the entry condition for nt-waiting trades.
        entry_cond types:
          "now"      → enter immediately
          "cb"       → close below level
          "ca"       → close above level
          "at"       → touch level
        """
        cond = row["entry_cond"]
        level = row["entry_level"]
        symbol = row["symbol"]

        price = get_spot(symbol)

        if cond == "now":
            return True
        if cond == "cb":
            return price < level
        if cond == "ca":
            return price > level
        if cond == "at":
            return abs(price - level) < 1e-6  # touch
        return False


    def _check_exit_condition(self, row: dict):
        """
        Returns:
           - None  → no exit
           - "sl"  → stop-loss triggered
           - "tp"  → take-profit triggered
        """
        price = get_spot(row["symbol"])

        # SL
        sl_cond = row["sl_cond"]
        sl_level = row["sl_level"]
        if sl_cond and sl_level is not None:
            if sl_cond == "cb" and price < sl_level:
                return "sl"
            if sl_cond == "ca" and price > sl_level:
                return "sl"
            if sl_cond == "at" and abs(price - sl_level) < 1e-6:
                return "sl"

        # TP
        tp_level = row["tp_level"]
        if tp_level is not None:
            # TP always uses direction of trade_type
            if row["trade_type"] == "long" and price >= tp_level:
                return "tp"
            if row["trade_type"] == "short" and price <= tp_level:
                return "tp"

        return None


    # ================================================================
    #  PROCESS EXISTING CACHE ENTRIES (ENTRY / EXIT / FORCE)
    # ================================================================

    def _process_cache(self):
        """
        Process all in-flight trades in cache.
        """
        to_process = list(self.cache.values())

        for entry in to_process:
            if entry.mode == "entry":
                self._process_entry(entry)
            elif entry.mode == "exit":
                self._process_exit(entry)
            elif entry.mode == "force_close":
                self._process_force_close(entry)


    # ================================================================
    #  SCAN ACTIVE_TRADES FOR NEW TASKS
    # ================================================================

    def _scan_for_tasks(self):
        """
        Find new tasks that should enter the cache:
           - manage = "C" → force close (any status)
           - nt-waiting + manage="Y" → entry check
           - nt-managing + manage="Y" → exit (SL/TP check)

        Skips rows already in cache.
        Skips pos-managing entirely.
        """

        res = sb.table("active_trades").select("*").execute()
        rows = res.data or []

        for row in rows:
            id_ = row["id"]

            # skip if already in cache
            if id_ in self.cache:
                continue

            manage = row["manage"]
            status = row["status"]

            # ============= FORCE CLOSE =============
            if manage == "C":
                self._start_force_close(row)
                continue

            # ============= SKIP NON-MANAGED =============
            if manage != "Y":
                continue

            # ============= SKIP pos-managing =============
            if status == "pos-managing":
                continue

            # ============= ENTRY (nt-waiting) =============
            if status == "nt-waiting":
                if self._check_entry_condition(row):
                    self._start_entry(row)
                continue

            # ============= EXIT (nt-managing) =============
            if status == "nt-managing":
                exit_reason = self._check_exit_condition(row)
                if exit_reason:
                    self._start_exit(row, exit_reason)
                continue


    # ================================================================
    #  MAIN RUN LOOP
    # ================================================================

    def run(self):
        """
        One iteration of manager:
          1. Process in-flight trades first (cache)
          2. Scan for new tasks
          3. Sleep lightly to avoid CPU hammering
        """

        # 1. process ongoing orders
        self._process_cache()

        # 2. scan for new tasks
        self._scan_for_tasks()

        # 3. light sleep
        time.sleep(0.15)   # adjustable (0.1–0.25)


# ================================================================
#  ENTRYPOINT FOR BOT
# ================================================================

def run_trade_manager():
    """
    Runs the trade manager in an infinite loop.
    """
    tm = TradeManager()
    log("info", "trade_manager_start", interval="~0.15s")

    while True:
        try:
            tm.run()
        except Exception as e:
            log("error", "trade_manager_exception", error=str(e))
            time.sleep(0.5)

