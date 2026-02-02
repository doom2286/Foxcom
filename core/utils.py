from datetime import datetime, timezone

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def parse_iso_utc(ts: str | None):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except:
        return None

def contains_disallowed_mentions(text: str) -> bool:
    t = (text or "").lower()
    if "@everyone" in t or "@here" in t:
        return True
    if "<@" in t or "<@&" in t or "<#" in t:
        return True
    if "@" in t:
        return True
    return False

