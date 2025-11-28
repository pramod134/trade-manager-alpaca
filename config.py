import os
from dataclasses import dataclass


@dataclass
class Settings:
    # Supabase
    supabase_url: str = os.environ.get("SUPABASE_URL", "")
    supabase_key: str = os.environ.get("SUPABASE_KEY", "")

    # Tradier
    tradier_live_base: str = os.environ.get("TRADIER_LIVE_BASE", "")
    tradier_live_token: str = os.environ.get("TRADIER_LIVE_TOKEN", "")
    tradier_account_id: str = os.environ.get("TRADIER_ACCOUNT_ID", "")

    # Trade manager loop interval (seconds)
    trade_manager_interval: float = float(os.environ.get("TRADE_MANAGER_INTERVAL", "0.5"))


settings = Settings()
