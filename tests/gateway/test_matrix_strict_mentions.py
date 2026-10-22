"""Room-specific strict mentions must not change other Matrix admission policies."""
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.matrix.adapter import MatrixAdapter, _apply_yaml_config

ROOM = "!bMTbleQprzVPuvgkco:eightstory.com"
BOT = "@forge:eightstory.com"


@pytest.fixture
def adapter():
    extra = _apply_yaml_config({}, {"strict_mention_rooms": [ROOM]}) or {}
    extra.update(user_id=BOT, require_mention=False, thread_require_mention=False,
                 auto_thread=False, free_response_rooms=[ROOM])
    bot = MatrixAdapter(PlatformConfig(enabled=True, extra=extra))
    bot._is_dm_room = AsyncMock(return_value=False)
    bot._resolve_room_identity = AsyncMock(return_value=MagicMock(display_name="Updates"))
    bot._get_display_name = AsyncMock(return_value="Andrew")
    bot._background_read_receipt = MagicMock()
    return bot


async def resolve(adapter, body, content=None, thread=False, room=ROOM, **kwargs):
    relates = {"rel_type": "m.thread", "event_id": "$root"} if thread else {}
    if thread:
        adapter._threads.mark("$root")
    return await adapter._resolve_message_context(
        room, "@andrew:eightstory.com", "$event", body,
        {"body": body, **(content or {})}, relates, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("thread", [False, True])
@pytest.mark.parametrize("body,content,allowed", [
    ("ordinary update", {}, False),
    ("/help", {}, False),
    ("@room update", {"m.mentions": {"room": True}}, False),
    ("ask forge", {}, False),
    ("@atlas:eightstory.com update", {"m.mentions": {"user_ids": ["@atlas:eightstory.com"]}}, False),
    ("@forge:other.example update", {}, False),
    ("@forge:eightstory.com.extra update", {}, False),
    ("pill", {"m.mentions": {"user_ids": [BOT]}}, True),
    (f"{BOT} update", {}, True),
    ("@forge update", {}, True),
    ("thanks @forge.", {}, True),
    ("thanks @forge:eightstory.com.", {}, True),
    ("@Forge update", {}, True),
    ("@Forge:other.example update", {}, False),
    ("@forge.extra update", {}, False),
    ("@forged update", {}, False),
    ("@forge-bot update", {}, False),
    ("forge@example.com update", {}, False),
    ("pill", {"formatted_body": f'<a href="https://matrix.to/#/{BOT}">Forge</a>'}, True),
    ("forge", {"m.mentions": {"user_ids": ["@atlas:eightstory.com"]}}, False),
])
async def test_strict_room_requires_this_bot(adapter, thread, body, content, allowed):
    ctx = await resolve(adapter, body, content, thread)
    assert (ctx is not None) == allowed


@pytest.mark.asyncio
async def test_dm_and_other_rooms_keep_free_response(adapter):
    assert await resolve(adapter, "ordinary update", room="!other:eightstory.com") is not None
    adapter._is_dm_room.return_value = True
    assert await resolve(adapter, "ordinary update") is not None


@pytest.mark.asyncio
async def test_allowlist_still_wins_over_direct_mention(adapter):
    adapter._allowed_rooms = {"!other:eightstory.com"}
    assert await resolve(adapter, BOT) is None


@pytest.mark.asyncio
async def test_strict_voice_can_be_claimed_by_followup_mention(adapter):
    assert await resolve(adapter, "voice.ogg", mention_claimed=True) is not None


def test_yaml_setting_is_profile_local(monkeypatch):
    monkeypatch.delenv("MATRIX_REQUIRE_MENTION", raising=False)
    assert _apply_yaml_config({}, {"strict_mention_rooms": [ROOM]}) == {"strict_mention_rooms": [ROOM]}
    assert _apply_yaml_config({}, {}) is None


@pytest.mark.parametrize("thread", [False, True])
def test_strict_voice_parks_even_with_free_response(adapter, thread):
    relates = {"rel_type": "m.thread", "event_id": "$root"} if thread else {}
    if thread:
        adapter._threads.mark("$root")
    assert adapter._voice_may_park(ROOM, "voice.ogg", {"msgtype": "m.audio", "org.matrix.msc3245.voice": {}}, relates, False)


@pytest.mark.asyncio
@pytest.mark.parametrize("thread", [False, True])
@pytest.mark.parametrize("name", ["helix", "forge"])
async def test_strict_room_strips_own_mention_from_command(adapter, thread, name):
    adapter._user_id = f"@{name}:eightstory.com"
    ctx = await resolve(adapter, f"@{name} /status", thread=thread)
    assert ctx is not None
    assert ctx[0] == "/status"


@pytest.mark.asyncio
@pytest.mark.parametrize("thread", [False, True])
@pytest.mark.parametrize("suffix", ["\n\nthanks", "", "\n> quoted continuation", "\n\n"])
async def test_reply_quote_is_not_a_direct_mention(adapter, thread, suffix):
    content = {
        "m.relates_to": {"m.in_reply_to": {"event_id": "$bot"}},
        "formatted_body": f'<mx-reply><a href="https://matrix.to/#/{BOT}">Forge</a></mx-reply>thanks',
    }
    assert await resolve(adapter, f"> <{BOT}> previous update{suffix}", content, thread) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("thread", [False, True])
async def test_parked_voice_requires_own_followup_mention(adapter, thread):
    sender = "@andrew:eightstory.com"
    relates = {"rel_type": "m.thread", "event_id": "$root"} if thread else {}
    root = "$root" if thread else None
    content = {"msgtype": "m.audio", "body": "voice.ogg", "org.matrix.msc3245.voice": {}}
    gate = adapter._parked_voices.begin(ROOM, sender, thread_id=root)
    assert await resolve(adapter, "voice.ogg", content, thread, voice_gate=gate) is None
    adapter._handle_media_message = AsyncMock()
    await adapter._handle_text_message(
        ROOM, sender, "$other", 0, {"body": "@atlas:eightstory.com"}, relates)
    adapter._handle_media_message.assert_not_awaited()
    await adapter._handle_text_message(ROOM, sender, "$own", 0, {"body": BOT}, relates)
    adapter._handle_media_message.assert_awaited_once_with(
        ROOM, sender, "$event", 0, content, relates, "m.audio", mention_claimed=True)


def test_setting_does_not_leak_to_another_adapter(adapter):
    other = MatrixAdapter(PlatformConfig(enabled=True, extra={"user_id": "@atlas:eightstory.com"}))
    assert adapter._strict_mention_rooms == {ROOM}
    assert other._strict_mention_rooms == set()
