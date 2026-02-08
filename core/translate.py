# core/translate.py
import re
import html
import aiohttp
from typing import Optional, Tuple

DETECT_URL = "https://translation.googleapis.com/language/translate/v2/detect"
TRANSLATE_URL = "https://translation.googleapis.com/language/translate/v2"

RE_CODEBLOCK = re.compile(r"```.*?```", re.DOTALL)
RE_INLINE_CODE = re.compile(r"`[^`]+`")
RE_MENTION = re.compile(r"<@!?\d+>|<@&\d+>|<#\d+>")
RE_URL = re.compile(r"https?://\S+")

def _protect(text: str) -> tuple[str, list[str]]:
    placeholders: list[str] = []

    def repl(m: re.Match) -> str:
        placeholders.append(m.group(0))
        return f"__PH_{len(placeholders)-1}__"

    # Order matters: protect big blocks first
    text = RE_CODEBLOCK.sub(repl, text)
    text = RE_INLINE_CODE.sub(repl, text)
    text = RE_MENTION.sub(repl, text)
    text = RE_URL.sub(repl, text)
    return text, placeholders

def _restore(text: str, placeholders: list[str]) -> str:
    for i, val in enumerate(placeholders):
        text = text.replace(f"__PH_{i}__", val)
    return text

async def _post_form(url: str, api_key: str, form: dict) -> dict:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, params={"key": api_key}, data=form) as resp:
            # Even on errors, Google often returns JSON
            return await resp.json()

async def detect_language(api_key: str, text: str) -> Optional[str]:
    data = await _post_form(DETECT_URL, api_key, {"q": text})
    try:
        return data["data"]["detections"][0][0]["language"]
    except Exception:
        return None

async def translate_to_english(api_key: str, text: str) -> Optional[str]:
    data = await _post_form(TRANSLATE_URL, api_key, {"q": text, "target": "en", "format": "text"})
    try:
        translated = data["data"]["translations"][0]["translatedText"]
        # v2 returns HTML-escaped entities sometimes
        return html.unescape(translated)
    except Exception:
        return None

async def maybe_translate_to_english(
    api_key: str,
    text: str,
    *,
    enabled: bool = True,
    min_chars: int = 12
) -> Tuple[str, Optional[str], bool, Optional[str]]:
    """
    Returns: (final_text, detected_lang, did_translate, original_text_if_translated)
    """
    if not enabled or not api_key:
        return text, None, False, None

    if not text or len(text.strip()) < min_chars:
        return text, None, False, None

    protected, placeholders = _protect(text)

    lang = await detect_language(api_key, protected)
    if not lang or lang.lower().startswith("en"):
        return text, lang, False, None

    translated = await translate_to_english(api_key, protected)
    if not translated:
        return text, lang, False, None

    translated = _restore(translated, placeholders)
    return translated, lang, True, text
