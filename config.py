import os
from dataclasses import dataclass


@dataclass
class Settings:
    # Supabase
    supabase_url: str = os.environ.get("SUPABASE_URL", "")
    supabase_key: str = os.environ.get("SUPABASE_KEY", "")

    # Tradier (LIVE) – kept for market data and any other services
    tradier_live_base: str = os.environ.get("TRADIER_LIVE_BASE", "")
    tradier_live_token: str = os.environ.get("TRADIER_LIVE_TOKEN", "")
    tradier_account_id: str = os.environ.get("TRADIER_ACCOUNT_ID", "")

    # Alpaca (PAPER) – used by trade-manager for order execution
    # Example:
    #   ALPACA_BASE  = https://paper-api.alpaca.markets
    #   ALPACA_KEY   = your paper API key
    #   ALPACA_SECRET= your paper secret
    alpaca_base: str = os.environ.get("ALPACA_BASE", "")
    alpaca_key: str = os.environ.get("ALPACA_KEY", "")
    alpaca_secret: str = os.environ.get("ALPACA_SECRET", "")



    # Trade manager loop interval (seconds)
    trade_manager_interval: float = float(
        os.environ.get("TRADE_MANAGER_INTERVAL", "1")
    )


settings = Settings()
