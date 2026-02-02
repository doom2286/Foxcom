# cogs/wordfilter.py
import json
import os
import re
import unicodedata
import discord
from discord.ext import commands

FILTER_PATH = os.path.join("data", "banned_words.json")

DEFAULT_CFG = {
    "enabled": True,
    # "contains" = substring match (strong but can false-positive)
    # "word"     = word-boundary match (safer)
    "mode": "word",
    "words": [],
}

# Common leetspeak / symbol substitutions (for MATCHING ONLY)
_LEET_MAP = str.maketrans({
    "0": "o",
    "1": "i",
    "!": "i",
    "|": "i",
    "3": "e",
    "4": "a",
    "@": "a",
    "5": "s",
    "$": "s",
    "7": "t",
    "+": "t",
    "8": "b",
    "9": "g",
})

# Zero-width + invisibles used to bypass filters
_ZERO_WIDTH = {
    "\u200b", "\u200c", "\u200d", "\ufeff",  # ZWSP/ZWNJ/ZWJ/BOM
    "\u2060", "\u00ad",                      # word joiner, soft hyphen
}


def _ensure_cfg_file():
    """Create data/banned_words.json with defaults if it doesn't exist."""
    os.makedirs(os.path.dirname(FILTER_PATH), exist_ok=True)
    if not os.path.exists(FILTER_PATH):
        with open(FILTER_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CFG, f, indent=2)


def load_cfg() -> dict:
    """
    Load filter config from JSON and merge with defaults so missing keys never crash.
    """
    _ensure_cfg_file()
    try:
        with open(FILTER_PATH, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except Exception:
        data = {}

    cfg = dict(DEFAULT_CFG)
    if isinstance(data, dict):
        cfg.update(data)

    cfg.setdefault("enabled", True)
    cfg.setdefault("mode", "word")
    cfg.setdefault("words", [])

    if not isinstance(cfg.get("words"), list):
        cfg["words"] = []
    if not isinstance(cfg.get("mode"), str):
        cfg["mode"] = "word"
    if not isinstance(cfg.get("enabled"), bool):
        cfg["enabled"] = True

    return cfg


def normalize(text: str) -> str:
    """
    Hard normalize for matching ONLY (does not alter broadcast content):
    - NFKC normalization (folds some compatibility lookalikes)
    - remove zero-width/invisible chars
    - strip accents/diacritics
    - map common leetspeak symbols to letters
    - remove non-alphanumerics (punctuation/emoji), preserve word separation
    - collapse whitespace
    """
    if not text:
        return ""

    # Unicode normalization
    text = unicodedata.normalize("NFKC", text)

    # Remove zero-width / invisibles
    text = "".join(ch for ch in text if ch not in _ZERO_WIDTH)

    # Lowercase
    text = text.lower()

    # Strip diacritics: é -> e
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", text)
        if unicodedata.category(ch) != "Mn"
    )

    # Leetspeak mapping
    text = text.translate(_LEET_MAP)

    # Keep only letters/numbers/spaces; turn everything else into space
    cleaned = []
    for ch in text:
        if ch.isalnum() or ch.isspace():
            cleaned.append(ch)
        else:
            cleaned.append(" ")

    text = "".join(cleaned)

    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


class WordFilterCog(commands.Cog):
    """
    Broadcast-only word filter helper.

    IMPORTANT:
    - This cog does NOT listen to on_message.
    - Nothing gets filtered in normal Discord chat.
    - You must call `check_text()` from your broadcast commands before sending.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.cfg = load_cfg()

    def reload_cfg(self):
        """Reload JSON config from disk."""
        self.cfg = load_cfg()

    def check_text(self, content: str) -> str | None:
        """
        Returns the matched banned word/phrase (original entry) if found, else None.

        Improvements:
        - punctuation/symbol insertion won't bypass (e.g. n!g_g3r)
        - spaced-out letters won't bypass for single-word entries (e.g. n i g g e r)
        """
        if not self.cfg.get("enabled", True):
            return None

        words = self.cfg.get("words", [])
        if not words:
            return None

        text = normalize(content)
        text_compact = text.replace(" ", "")
        mode = (self.cfg.get("mode") or "word").lower()

        if mode == "contains":
            for w in words:
                ww = normalize(str(w))
                if not ww:
                    continue

                if ww in text:
                    return str(w)

                # compact contains to prevent spaced-out bypass
                ww_compact = ww.replace(" ", "")
                if len(ww_compact) >= 4 and ww_compact in text_compact:
                    return str(w)
            return None

        # "word" mode: word-boundary matching; supports multi-word phrases too
        for w in words:
            ww = normalize(str(w))
            if not ww:
                continue

            pattern = r"(?<!\w)" + re.escape(ww) + r"(?!\w)"
            if re.search(pattern, text):
                return str(w)

            # Extra: catch spaced-out single-word entries only
            if " " not in ww:
                ww_compact = ww
                if len(ww_compact) >= 4 and ww_compact in text_compact:
                    return str(w)

        return None


async def setup(bot: commands.Bot):
    await bot.add_cog(WordFilterCog(bot))

