import threading
from trade_manager import run_trade_manager
from alpaca_ws_client import run_alpaca_ws_forever


def start_ws_client():
    """Run Alpaca WebSocket client in its own thread."""
    run_alpaca_ws_forever()


if __name__ == "__main__":
    # Start WS client in background
    ws_thread = threading.Thread(target=start_ws_client, daemon=True)
    ws_thread.start()

    # Start trade manager in main thread
    run_trade_manager()
