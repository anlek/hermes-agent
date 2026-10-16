"""Matrix E2EE crypto state-store shim handed to mautrix's OlmMachine."""

from __future__ import annotations

import logging
from contextlib import suppress
from typing import Any

logger = logging.getLogger(__name__)


class _CryptoStateStore:
    """Olm state store with homeserver fallback for cold encryption caches.

    Missing encryption state means plaintext only after an authoritative M_NOT_FOUND;
    transport or permission failures must never downgrade encrypted delivery.
    """

    def __init__(self, client_state_store: Any, joined_rooms: set, client=None):
        self._ss = client_state_store
        self._joined_rooms = joined_rooms
        self._client = client
        # MemoryStateStore has no set_encryption_info, so cache homeserver answers here.
        self._enc_info_cache: dict = {}

    async def is_encrypted(self, room_id: str) -> bool:
        return (await self.get_encryption_info(room_id)) is not None

    async def get_encryption_info(self, room_id: str):
        info = await self._ss.get_encryption_info(room_id) if hasattr(self._ss, "get_encryption_info") else None
        if info is not None:
            return info
        if room_id in self._enc_info_cache:
            return self._enc_info_cache[room_id]
        if self._client is None:
            raise RuntimeError("Room encryption state is unavailable")
        try:
            from mautrix.types import EventType as _ET, RoomEncryptionStateEventContent as _Enc, RoomID as _RID
            raw = await self._client.get_state_event(_RID(room_id), _ET.ROOM_ENCRYPTION)
        except Exception as exc:
            from mautrix.errors import MNotFound
            if isinstance(exc, MNotFound):
                return None
            raise
        if not raw:
            raise RuntimeError("Invalid room encryption state")
        content = raw if isinstance(raw, _Enc) else _Enc.deserialize(
            raw.serialize() if hasattr(raw, "serialize") else raw)
        if hasattr(self._ss, "set_encryption_info"):
            with suppress(Exception):
                await self._ss.set_encryption_info(_RID(room_id), content)
        self._enc_info_cache[room_id] = content
        return content

    async def find_shared_rooms(self, user_id: str) -> list:
        return list(self._joined_rooms)  # all joined rooms: correct for a single-user bot
