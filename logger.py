
import json
from datetime import datetime, timezone


def log(level: str, event: str, **fields):
    ts = datetime.now(timezone.utc).isoformat()
    entry = {
        "ts": ts,
        "level": level,
        "event": event,
        **fields,
    }
    print(json.dumps(entry), flush=True)
