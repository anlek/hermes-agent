"""Direct mentions for opt-in strict rooms, separate from legacy name matching."""
from __future__ import annotations

import re
from typing import TYPE_CHECKING, cast

from plugins.platforms.matrix.voice_mention import is_voice_event

if TYPE_CHECKING:
    from plugins.platforms.matrix.adapter import MatrixAdapter


def directly_mentions_bot(body: str, content: dict, user_id: str) -> bool:
    if not user_id:
        return False
    mentions = content.get("m.mentions")
    if isinstance(mentions, dict):
        return user_id in (mentions.get("user_ids") or [])
    # The adapter supplies only user text, excluding any plain-text reply fallback.
    formatted = re.sub(r"<mx-reply\b[^>]*>.*?</mx-reply>", "",
                       content.get("formatted_body") or "", flags=re.DOTALL | re.IGNORECASE)
    localpart = user_id.partition(":")[0]
    return bool(
        re.search(r"(?<![\w@])" + re.escape(user_id) + r"(?![\w:-]|\.\w)", body)
        or re.search(r"(?<![\w@])" + re.escape(localpart) + r"(?![\w:-]|\.\w)", body, re.IGNORECASE)
        or re.search(r"https://matrix\.to/#/" + re.escape(user_id) + r"(?=[\"'?#])", formatted)
    )


class MatrixMentionPolicy:
    """Room mention and voice-parking policy shared by the adapter's ingress paths."""

    def _voice_may_park(self, room_id: str, body: str, content: dict, relates_to: dict,
                        mention_claimed: bool) -> bool:
        """Pre-check admission before awaiting DM identity; release a DM's mark once known."""
        from plugins.platforms.matrix.adapter import _thread_root

        adapter = cast("MatrixAdapter", self)
        strict = room_id in adapter._strict_mention_rooms
        if mention_claimed or not (adapter._require_mention or strict) or not is_voice_event(content):
            return False
        if strict:
            return not (adapter._allowed_rooms and room_id not in adapter._allowed_rooms) and not adapter._content_mentions_bot(
                body, content, strict=True)
        if room_id in adapter._free_rooms or (adapter._allowed_rooms and room_id not in adapter._allowed_rooms):
            return False
        thread_id = _thread_root(relates_to)
        if thread_id and thread_id in adapter._threads:
            return False
        return not body.startswith("/") and not adapter._content_mentions_bot(body, content)

    def _content_mentions_bot(self, body: str, content: dict, *, strict: bool = False) -> bool:
        """Use strict direct mentions only in opted-in rooms, retaining legacy matching elsewhere."""
        from plugins.platforms.matrix.adapter import _split_reply_fallback

        adapter = cast("MatrixAdapter", self)
        if strict:
            if (content.get("m.relates_to") or {}).get("m.in_reply_to"):
                _, body = _split_reply_fallback(body)
            return directly_mentions_bot(body, content, adapter._user_id)
        mentions = content.get("m.mentions") or {}
        return adapter._is_bot_mentioned(
            body, content.get("formatted_body"), mentions.get("user_ids") if isinstance(mentions, dict) else None)
