import json
import time
from typing import Any, Optional

import websocket  # pip install websocket-client

from config import settings
from logger import log
import supabase_client


def _ws_url() -> str:
    """
    Build the Alpaca trading WebSocket URL from ALPACA_BASE.

    Example:
      ALPACA_BASE = https://paper-api.alpaca.markets
      => wss://paper-api.alpaca.markets/stream
    """
    base = (settings.alpaca_base or "").strip()
    if base.startswith("https://"):
        base = base[len("https://") :]
    elif base.startswith("http://"):
        base = base[len("http://") :]
    base = base.rstrip("/")
    return f"wss://{base}/stream"


def _update_order_status_in_db(
    order_id: str,
    status: Optional[str],
    comment: Optional[str],
) -> None:
    """
    Update active_trades row(s) that match this Alpaca order_id.

    We only touch:
      - order_status
      - comment

    Your trade_manager already reads these fields and avoids
    re-sending orders if status is non-terminal.
    """
    if not order_id:
        return

    update: dict[str, Any] = {}
    if status is not None:
        update["order_status"] = status
    if comment is not None:
        update["comment"] = comment

    if not update:
        return

    try:
        sb = supabase_client.get_client()
        # Match by order_id (this is what trade_manager writes when placing orders)
        resp = sb.table("active_trades").update(update).eq("order_id", order_id).execute()
        log(
            "info",
            "alpaca_ws_db_update",
            order_id=order_id,
            update=update,
            resp=resp,
        )
    except Exception as e:
        log(
            "error",
            "alpaca_ws_db_update_error",
            order_id=order_id,
            update=update,
            error=str(e),
        )


def _handle_trade_update(payload: dict[str, Any]) -> None:
    """
    Handle a single trade_updates message.

    Example payload (simplified):

      {
        "event": "fill",
        "order": {
          "id": "9048-...",
          "status": "filled",
          ...
        },
        "position_qty": "1",
        "timestamp": "..."
      }
    """
    event = payload.get("event")
    order = payload.get("order") or {}

    order_id = order.get("id")
    status = order.get("status")

    if not order_id:
        log("error", "alpaca_ws_missing_order_id", payload=payload)
        return

    # We use `event` as comment, so you can see "new", "fill", "canceled", etc.
    comment = event
    _update_order_status_in_db(order_id=order_id, status=status, comment=comment)

    log(
        "info",
        "alpaca_ws_trade_update",
        order_id=order_id,
        status=status,
        event=event,
    )


def _on_message(ws: websocket.WebSocketApp, message: Any) -> None:
    try:
        data = json.loads(message)
    except Exception as e:
        log("error", "alpaca_ws_json_parse_error", raw=str(message)[:500], error=str(e))
        return

    stream = data.get("stream")
    payload = data.get("data") or {}

    if stream == "authorization":
        log("info", "alpaca_ws_authorization", data=payload)
        return

    if stream == "listening":
        log("info", "alpaca_ws_listening", data=payload)
        return

    if stream == "trade_updates":
        _handle_trade_update(payload)
        return

    # Other streams (if any) – just log
    log("info", "alpaca_ws_unknown_stream", stream=stream, data=payload)


def _on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
    log("error", "alpaca_ws_error", error=str(error))


def _on_close(
    ws: websocket.WebSocketApp,
    code: Optional[int],
    msg: Optional[str],
) -> None:
    log("info", "alpaca_ws_closed", code=code, msg=msg)


def _on_open(ws: websocket.WebSocketApp) -> None:
    """
    Authenticate and subscribe to trade_updates.

    This follows Alpaca's documented JSON protocol:

      { "action": "auth", "key": "...", "secret": "..." }
      { "action": "listen", "data": { "streams": ["trade_updates"] } }
    """
    auth_msg = {
        "action": "auth",
        "key": settings.alpaca_key or "",
        "secret": settings.alpaca_secret or "",
    }
    ws.send(json.dumps(auth_msg))

    listen_msg = {
        "action": "listen",
        "data": {"streams": ["trade_updates"]},
    }
    ws.send(json.dumps(listen_msg))

    log("info", "alpaca_ws_open", url=_ws_url())


def run_alpaca_ws_forever() -> None:
    """
    Main loop to keep the Alpaca trade_updates websocket alive.

    Run this as a separate process/worker (e.g. second Railway service):

      python -m alpaca_ws_client

    or

      python alpaca_ws_client.py
    """
    url = _ws_url()
    log("info", "alpaca_ws_start", url=url)

    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )
            ws.run_forever()
        except Exception as e:
            log("error", "alpaca_ws_run_error", error=str(e))

        # Simple backoff before reconnect
        time.sleep(5)


if __name__ == "__main__":
    run_alpaca_ws_forever()
