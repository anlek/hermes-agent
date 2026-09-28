"""A bare mention authorizes a parked voice only in its original conversation lane."""

import asyncio
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

from gateway.config import PlatformConfig
from gateway.platforms.event import MessageType
from plugins.platforms.matrix.adapter import MatrixAdapter
from plugins.platforms.matrix import voice_mention


ROOM = "!voice:example.org"
SENDER = "@alice:example.org"
BOT = "@hermes:example.org"


@pytest_asyncio.fixture
async def adapter():
    instance = MatrixAdapter(PlatformConfig(enabled=True, extra={
        "homeserver": "https://matrix.example.org", "user_id": BOT,
        "require_mention": True, "session_scope": "room",
    }))
    instance._text_batch_delay_seconds = 0
    instance._startup_ts = time.time() - 10
    instance._resolve_room_identity = AsyncMock(return_value=SimpleNamespace(
        display_name="Voice room", room_topic=None, server_name="example.org"))
    instance._is_dm_room = AsyncMock(return_value=False)
    instance._get_display_name = AsyncMock(return_value="Alice")
    instance._background_read_receipt = Mock()
    instance._client = SimpleNamespace(download_media=AsyncMock(return_value=b"voice payload"))
    instance.handle_message = AsyncMock()
    yield instance
    await instance.cancel_background_tasks()


def event(event_id, thread_id, *, voice=False, room=ROOM, sender=SENDER):
    content = ({"msgtype": "m.audio", "body": "voice.ogg", "m.mentions": {},
                "org.matrix.msc3245.voice": {}, "info": {"mimetype": "audio/ogg"},
                "url": "mxc://example.org/" + event_id.lstrip("$")}
               if voice else {"msgtype": "m.text", "body": BOT,
                              "m.mentions": {"user_ids": [BOT]}})
    if thread_id is not None:
        content["m.relates_to"] = {
            "rel_type": "m.thread", "event_id": thread_id,
            # Replies within one thread need not reference the same immediate event.
            "m.in_reply_to": {"event_id": event_id + "-previous"},
        }
    return SimpleNamespace(room_id=room, sender=sender, event_id=event_id,
                           timestamp=int(time.time() * 1000), content=content)


@pytest.mark.asyncio
@pytest.mark.parametrize("encrypted", [False, True], ids=["plain", "encrypted"])
@pytest.mark.parametrize("voice_lane, mention_lane, mention_room, mention_sender", [
    ("$thread-a", "$thread-b", ROOM, SENDER),
    (None, "$thread-a", ROOM, SENDER),
    ("$thread-a", None, ROOM, SENDER),
    ("$thread-a", "$thread-a", ROOM, SENDER),
    (None, None, ROOM, SENDER),
    ("$thread-a", "$thread-a", ROOM, "@bob:example.org"),
    ("$thread-a", "$thread-a", "!other:example.org", SENDER),
], ids=["cross-thread", "room-to-thread", "thread-to-room", "same-thread",
        "same-room-root", "other-sender", "other-room"])
async def test_voice_claim_requires_same_lane_and_sender(
        adapter, encrypted, voice_lane, mention_lane, mention_room, mention_sender):
    voice = event("$voice", voice_lane, voice=True)
    plaintext = b"voice payload"
    if encrypted:
        encrypt_attachment = pytest.importorskip("mautrix.crypto.attachments").encrypt_attachment
        ciphertext, attachment = encrypt_attachment(plaintext)
        voice.content["file"] = attachment.serialize()
        voice.content["file"]["url"] = voice.content.pop("url")
        adapter._client.download_media.return_value = ciphertext

    await adapter._on_room_message(voice)
    adapter._client.download_media.assert_not_awaited()
    adapter.handle_message.assert_not_awaited()
    await adapter._on_room_message(event(
        "$mention", mention_lane, room=mention_room, sender=mention_sender))

    matches = (voice_lane == mention_lane and mention_room == ROOM and mention_sender == SENDER)
    if not matches:
        adapter._client.download_media.assert_not_awaited()
        message = adapter.handle_message.await_args.args[0]
        assert message.message_id == "$mention"
        assert message.source.thread_id == mention_lane
        assert message.source.chat_id == mention_room
        # A rejected claim must leave the voice available to its own lane and sender.
        await adapter._on_room_message(event("$correct-mention", voice_lane))

    messages = [call.args[0] for call in adapter.handle_message.await_args_list]
    assert [message.message_id for message in messages] == (
        ["$voice"] if matches else ["$mention", "$voice"])
    claimed = messages[-1]
    assert claimed.message_type == MessageType.VOICE
    assert claimed.source.thread_id == voice_lane
    assert claimed.source.chat_id == ROOM
    assert claimed.source.user_id == SENDER
    assert Path(claimed.media_urls[0]).read_bytes() == plaintext
    adapter._client.download_media.assert_awaited_once_with("mxc://example.org/voice")


@pytest.mark.asyncio
@pytest.mark.parametrize("slow_lane, other_lane", [
    ("$thread-a", "$thread-b"), (None, "$thread-b"), ("$thread-a", None),
], ids=["cross-thread", "room-to-thread", "thread-to-room"])
@pytest.mark.parametrize("settle_timeout", [5.0, 0.0], ids=["settle", "timeout"])
async def test_concurrent_claims_do_not_wait_for_or_discard_other_lanes(
        adapter, monkeypatch, slow_lane, other_lane, settle_timeout):
    """A claim's waiting, arrival cutoff and late-gate floor must all stay lane-local."""
    monkeypatch.setattr(voice_mention, "SETTLE_TIMEOUT_SECONDS", settle_timeout)
    gate_started, finish_gate = asyncio.Event(), asyncio.Event()
    identity = adapter._resolve_room_identity.return_value

    async def resolve_identity(_room):
        if not gate_started.is_set():
            gate_started.set()
            await finish_gate.wait()
        return identity

    adapter._resolve_room_identity.side_effect = resolve_identity
    tasks = [asyncio.create_task(adapter._on_room_message(event("$slow", slow_lane, voice=True)))]
    try:
        await asyncio.wait_for(gate_started.wait(), timeout=2)
        await adapter._on_room_message(event("$other", other_lane, voice=True))
        adapter._client.download_media.assert_not_awaited()
        # Must not wait for the unrelated gate, even when its settlement has not timed out.
        await asyncio.wait_for(adapter._on_room_message(event("$other-mention", other_lane)), timeout=2)
        assert [c.args[0].message_id for c in adapter.handle_message.await_args_list] == ["$other"]
        adapter._client.download_media.assert_awaited_once_with("mxc://example.org/other")

        if settle_timeout:
            settling = asyncio.Event()
            settle = adapter._parked_voices.settle

            async def observed_settle(*args, **kwargs):
                settling.set()
                await settle(*args, **kwargs)

            monkeypatch.setattr(adapter._parked_voices, "settle", observed_settle)
            tasks.append(asyncio.create_task(adapter._on_room_message(event("$slow-mention", slow_lane))))
            await asyncio.wait_for(settling.wait(), timeout=2)
            assert not tasks[-1].done()
            # This later voice must survive the earlier mention's arrival cutoff.
            await adapter._on_room_message(event("$later", slow_lane, voice=True))
            finish_gate.set()
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=2)
            await adapter._on_room_message(event("$later-mention", slow_lane))
            expected = ["$other", "$slow", "$later"]
        else:
            # Claiming the newer voice in another lane cannot suppress this late older gate.
            finish_gate.set()
            await asyncio.wait_for(tasks[0], timeout=2)
            adapter._client.download_media.assert_awaited_once_with("mxc://example.org/other")
            await adapter._on_room_message(event("$slow-mention", slow_lane))
            expected = ["$other", "$slow"]

        messages = [c.args[0] for c in adapter.handle_message.await_args_list]
        assert [message.message_id for message in messages] == expected
        assert [message.source.thread_id for message in messages] == [other_lane] + [slow_lane] * (len(expected) - 1)
        assert [c.args[0] for c in adapter._client.download_media.await_args_list] == [
            "mxc://example.org/" + event_id.lstrip("$") for event_id in expected]
        assert not adapter._parked_voices._parked
        assert not adapter._parked_voices._inflight
        assert not adapter._parked_voices._floor
    finally:
        finish_gate.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
