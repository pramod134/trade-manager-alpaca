import threading
from trade_manager import run_trade_manager, run_trade_updater


def start_trade_updater():
    """Run trade updater in its own thread."""
    run_trade_updater()


if __name__ == "__main__":
    # Start trade updater in background
    updater_thread = threading.Thread(target=start_trade_updater, daemon=True)
    updater_thread.start()

    # Start trade manager in main thread
    run_trade_manager()
