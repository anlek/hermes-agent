"""Contracts at the upstream voice-gate / fork trusted-source merge boundary."""

import asyncio
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.event import MessageType
from gateway.session import build_session_key
from plugins.platforms.matrix.adapter import MatrixAdapter


ROOM = "!voice:example.org"
SENDER = "@alice:example.org"
BOT = "@hermes:example.org"


@pytest.fixture
def adapter():
    instance = MatrixAdapter(PlatformConfig(enabled=True, extra={
        "homeserver": "https://matrix.example.org",
        "user_id": BOT,
        "require_mention": True,
        "session_scope": "room",
    }))
    instance._text_batch_delay_seconds = 0
    instance._startup_ts = time.time() - 10
    instance._resolve_room_identity = AsyncMock(return_value=SimpleNamespace(
        display_name="Voice room", room_topic="Shared topic", server_name="example.org"))
    instance._is_dm_room = AsyncMock(return_value=False)
    instance._get_display_name = AsyncMock(return_value="Alice from Matrix")
    instance._background_read_receipt = Mock()
    instance.handle_message = AsyncMock()
    return instance


def event(event_id, content):
    return SimpleNamespace(room_id=ROOM, sender=SENDER, event_id=event_id,
                           timestamp=int(time.time() * 1000), content=content)


def voice_content():
    return {"msgtype": "m.audio", "body": "voice.ogg", "m.mentions": {},
            "org.matrix.msc3245.voice": {}, "info": {"mimetype": "audio/ogg"}}


@pytest.mark.asyncio
async def test_dm_voice_releases_gate_before_native_source_lookup(adapter):
    """A DM cannot park; slow source construction must not stall the next bare mention."""
    adapter._is_dm_room.return_value = True
    lookup_started, finish_lookup = asyncio.Event(), asyncio.Event()

    async def display_name(*_args):
        if not lookup_started.is_set():
            lookup_started.set()
            await finish_lookup.wait()
        return "Alice from Matrix"

    adapter._get_display_name.side_effect = display_name
    voice_task = asyncio.create_task(adapter._on_room_message(event("$dm-voice", voice_content())))
    try:
        await asyncio.wait_for(lookup_started.wait(), timeout=2)
        assert not adapter._parked_voices.pending(ROOM, SENDER)
        await asyncio.wait_for(adapter._on_room_message(event("$mention", {
            "msgtype": "m.text", "body": BOT, "m.mentions": {"user_ids": [BOT]},
        })), timeout=2)
        assert [call.args[0].message_id for call in adapter.handle_message.await_args_list] == ["$mention"]
        finish_lookup.set()
        await asyncio.wait_for(voice_task, timeout=2)
        messages = [call.args[0] for call in adapter.handle_message.await_args_list]
        assert [message.message_id for message in messages] == ["$mention", "$dm-voice"]
        assert messages[-1].source.chat_type == "dm"
        assert messages[-1].source.thread_id is None
        assert not adapter._parked_voices.pending(ROOM, SENDER)
    finally:
        finish_lookup.set()
        voice_task.cancel()
        await asyncio.gather(voice_task, return_exceptions=True)
        await adapter.cancel_background_tasks()


@pytest.mark.asyncio
@pytest.mark.parametrize("session_scope", ["room", "thread"])
async def test_claimed_encrypted_voice_and_trusted_turn_share_source_policy(adapter, session_scope):
    """Claiming decrypts once, keeps the voice's lane, and leaves trusted names intact."""
    encrypt_attachment = pytest.importorskip("mautrix.crypto.attachments").encrypt_attachment
    adapter._matrix_session_scope = session_scope
    plaintext = b"private voice payload"
    ciphertext, attachment = encrypt_attachment(plaintext)
    encrypted_file = attachment.serialize()
    encrypted_file["url"] = "mxc://example.org/encrypted-voice"
    content = voice_content()
    content["file"] = encrypted_file
    adapter._client = SimpleNamespace(download_media=AsyncMock(return_value=ciphertext))
    try:
        await adapter._on_room_message(event("$voice", content))
        adapter._client.download_media.assert_not_awaited()
        adapter.handle_message.assert_not_awaited()
        assert adapter._parked_voices.pending(ROOM, SENDER)

        await adapter._on_room_message(event("$mention", {
            "msgtype": "m.text", "body": BOT, "m.mentions": {"user_ids": [BOT]},
        }))
        adapter.handle_message.assert_awaited_once()
        message = adapter.handle_message.await_args.args[0]
        assert message.message_id == "$voice"
        assert message.message_type == MessageType.VOICE
        assert Path(message.media_urls[0]).read_bytes() == plaintext
        adapter._client.download_media.assert_awaited_once_with(encrypted_file["url"])
        assert not adapter._parked_voices.pending(ROOM, SENDER)
        assert message.source.thread_id == ("$voice" if session_scope == "thread" else None)
        adapter._background_read_receipt.assert_any_call(ROOM, "$voice")
        adapter._background_read_receipt.assert_any_call(ROOM, "$mention")

        adapter._get_display_name.reset_mock()
        trusted = await adapter.build_trusted_turn_source(
            room_id=ROOM, sender=SENDER, event_id="$voice", user_name="Alice from relay")
        adapter._get_display_name.assert_not_awaited()
        assert trusted.user_name == "Alice from relay"
        assert message.source.user_name == "Alice from Matrix"
        assert build_session_key(trusted) == build_session_key(message.source)
        assert trusted.chat_topic == message.source.chat_topic == "Shared topic"
        assert trusted.guild_id == message.source.guild_id == "example.org"
        assert trusted.parent_chat_id == message.source.parent_chat_id
    finally:
        await adapter.cancel_background_tasks()
