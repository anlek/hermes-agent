"""Current gateway integration contracts for the native dictation clients."""

import asyncio
import base64
import io
import json
import wave
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp
import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig, StreamingConfig
from gateway.platforms.api_server import APIServerAdapter
from gateway.run import GatewayRunner
from gateway.run_turn_runner import TurnRunner
from gateway.trusted_matrix_turn import HELIX_SUPPORT_ROOM_ID, ExternalTurnUnavailable
from plugins.platforms.matrix.adapter import MatrixAdapter, _CryptoStateStore
from mautrix.client.state_store.memory import MemoryStateStore

KEY = "dictation-test-ingress-key-32-bytes-long"


def make_gateway():
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig()
    matrix = MatrixAdapter(PlatformConfig(enabled=True, typing_indicator=False))
    matrix.gateway_runner = runner
    matrix._mark_connected()
    matrix._closing = False
    matrix._encryption = True
    matrix._crypto_db = object()
    matrix._matrix_session_scope = "room"
    matrix._dm_mention_threads = False
    matrix._client = SimpleNamespace(
        crypto=SimpleNamespace(),
        device_id="test-device",
        redact=AsyncMock(),
        state_store=MemoryStateStore(),
        get_state_event=AsyncMock(return_value={"algorithm": "m.megolm.v1.aes-sha2"}),
        send_message_event=AsyncMock(
            side_effect=lambda *args: (
                "$input"
                if args[2].get("body", "").startswith("Andrew (spoken):")
                else "$answer"
            )
        ),
    )
    matrix._client.crypto.state_store = _CryptoStateStore(
        matrix._client.state_store, set(), matrix._client
    )
    matrix._resolve_room_identity = AsyncMock(
        return_value=SimpleNamespace(
            display_name="Voice Memos", room_topic="", server_name="eightstory.com"
        )
    )
    matrix._is_dm_room = AsyncMock(return_value=False)
    runner.adapters = {Platform.MATRIX: matrix}
    return runner, matrix


@pytest.mark.asyncio
@pytest.mark.parametrize("streaming", [False, True])
async def test_native_client_contract_through_registered_http_route(
    monkeypatch, unused_tcp_port, streaming
):
    """Real HTTP registration → ingress → runner → native Matrix text delivery, plus WAV/SSE."""
    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", KEY)
    runner, matrix = make_gateway()
    runner.config.streaming = StreamingConfig(enabled=True)
    questions = []

    def text_sends():
        return [
            call
            for call in matrix._client.send_message_event.await_args_list
            if "body" in call.args[2]
        ]

    async def model_turn(event):
        questions.append(event.text)
        # Exercise the current stream wiring while native Matrix previews are enabled.
        ctx = SimpleNamespace(
            source=event.source,
            streaming_tts_consumer_holder=[None],
            user_config={},
            resolve_display_setting=lambda *_args: True,
            interim_assistant_messages_enabled=True,
            _run_still_current=lambda: True,
        )
        consumer, delta, interim, want_interim = TurnRunner(
            runner, ctx
        )._setup_stream_consumer("matrix")
        assert consumer is None and want_interim is False
        interim("Do not duplicate this in Matrix")
        if delta:
            delta("Provisional")
        return "Exact final answer"

    matrix.set_message_handler(model_turn)
    api = runner._create_adapter(
        Platform.API_SERVER,
        PlatformConfig(
            enabled=True,
            extra={
                "key": "separate-api-key-for-test",
                "host": "127.0.0.1",
                "port": unused_tcp_port,
            },
        ),
    )
    assert isinstance(api, APIServerAdapter)
    audio = io.BytesIO()
    with wave.open(audio, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(24000)
        wav.writeframes(b"\x00\x00" * 32)
    api._trusted_matrix_turn_ingress._wav_synthesizer = AsyncMock(
        return_value=audio.getvalue()
    )
    assert await api.connect()
    payload = dict(
        request_id="native-client", text="Hello Helix", tts=True, audio_format="wav"
    )
    if streaming:
        payload["stream"] = True
    url = f"http://127.0.0.1:{unused_tcp_port}/v1/platform-turns/matrix"
    try:
        async with aiohttp.ClientSession() as client:
            async with client.post(url, json=payload) as denied:
                assert denied.status == 401
            assert not text_sends()
            async with client.post(
                url, json=payload, headers={"Authorization": f"Bearer {KEY}"}
            ) as response:
                assert response.status == 200
                assert response.headers["Cache-Control"] == "no-store"
                if streaming:
                    frames = await response.text()
                    assert "event: delta" in frames and "event: replace" in frames
                    done = frames.split("event: done\n", 1)[1].split("\n\n", 1)[0]
                    body = json.loads(done.removeprefix("data: "))
                else:
                    body = await response.json()
            assert body["ok"] is True
            assert body["answer"] == "Exact final answer"
            assert body["matrix"] == {
                "input_event_id": "$input",
                "reply_event_id": "$answer",
            }
            assert base64.b64decode(body["audio"]["data_base64"]) == audio.getvalue()
            assert questions == ["Andrew (spoken): Hello Helix"]
            calls = text_sends()
            assert len(calls) == 2
            assert calls[0].args[2]["body"] == questions[0]
            assert calls[1].args[2]["body"] == body["answer"]
            # Same request ID is replayed without repeating Matrix/model side effects.
            async with client.post(
                url, json=payload, headers={"Authorization": f"Bearer {KEY}"}
            ) as replay:
                assert replay.status == 200
                await replay.read()
            assert len(text_sends()) == 2
            async with client.post(
                url.replace("/v1/", "/p/default/v1/"),
                json=payload,
                headers={"Authorization": f"Bearer {KEY}"},
            ) as prefixed:
                assert prefixed.status in (400, 404)
    finally:
        await matrix.cancel_background_tasks()
        await api.disconnect()


@pytest.mark.asyncio
async def test_refused_native_admission_does_not_wait_for_http_timeout():
    runner, matrix = make_gateway()
    # The real adapter refuses admission with no message handler. Make it disappear
    # during the transport await after preflight, as during a disconnect/replacement.
    matrix.set_message_handler(AsyncMock(return_value="never runs"))

    async def send_then_disconnect(*_args):
        matrix.set_message_handler(None)
        return "$input"

    matrix._client.send_message_event.side_effect = send_then_disconnect
    from gateway.platforms.base import MessageEvent
    from gateway.session import SessionSource

    event = MessageEvent(
        text="hello",
        source=SessionSource(
            platform=Platform.MATRIX, chat_id=HELIX_SUPPORT_ROOM_ID, chat_type="group"
        ),
    )
    with pytest.raises(ExternalTurnUnavailable, match="did not accept"):
        await asyncio.wait_for(
            runner.process_external_turn("refused-admission", event), timeout=2
        )
    assert runner._external_turn_futures == {}
    assert not matrix._session_tasks


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [TimeoutError("offline"), ValueError("invalid state")]
)
async def test_cold_encryption_cache_fails_closed_for_readiness_and_media(failure):
    _, matrix = make_gateway()
    matrix._client.get_state_event.side_effect = failure
    matrix._client.upload_media = AsyncMock()
    ready, _ = await matrix.trusted_turn_readiness(HELIX_SUPPORT_ROOM_ID)
    assert ready is False
    result = await matrix._upload_and_send(
        HELIX_SUPPORT_ROOM_ID, b"private", "private.txt", "text/plain", "m.file"
    )
    assert result.success is False
    matrix._client.upload_media.assert_not_awaited()
    matrix._client.send_message_event.assert_not_awaited()


@pytest.mark.asyncio
async def test_trusted_answer_cannot_succeed_with_truncated_formatting_fallback():
    _, matrix = make_gateway()
    from gateway.platforms.base import SendResult

    matrix.send = AsyncMock(
        side_effect=[
            SendResult(success=False, error="invalid format"),
            SendResult(success=True, message_id="$truncated"),
        ]
    )
    result = await matrix._send_with_retry(
        HELIX_SUPPORT_ROOM_ID,
        "answer " * 1000,
        metadata={"_external_turn_cancel_check": lambda: False},
    )
    assert result.success is False
    matrix.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_native_debounce_preserves_trusted_prompt_and_drains_separately():
    _, matrix = make_gateway()
    from gateway.platforms.base import MessageEvent
    from gateway.session import SessionSource, build_session_key

    source = SessionSource(
        platform=Platform.MATRIX,
        chat_id=HELIX_SUPPORT_ROOM_ID,
        chat_type="group",
        user_id="@andrew:eightstory.com",
    )
    trusted = MessageEvent(
        text="Andrew (spoken): original",
        source=source,
        internal=True,
        metadata={"external_turn_request_id": "queued"},
    )
    native = MessageEvent(text="unrelated native text", source=source)
    key = build_session_key(source)
    matrix._pending_messages[key] = trusted
    try:
        await matrix._queue_text_debounce(key, native)
        assert await matrix._flush_text_debounce_now(key) is False
        assert trusted.text == "Andrew (spoken): original"
        assert matrix._pending_messages.pop(key) is trusted
        assert await matrix._flush_text_debounce_now(key) is True
        assert matrix._pending_messages[key] is native
        assert native.text == "unrelated native text"
    finally:
        matrix._discard_text_debounce(key)
        matrix._pending_messages.clear()


@pytest.mark.asyncio
@pytest.mark.parametrize("guard", ["paused", "proxy"])
async def test_unavailable_execution_blocks_new_and_queued_spoken_work(
    monkeypatch, tmp_path, guard
):
    runner, matrix = make_gateway()
    from gateway.platforms.base import MessageEvent
    from gateway.session import SessionSource
    from agent import estop

    sentinel = tmp_path / "ESTOP"
    monkeypatch.setattr(estop, "_candidate_sentinel_paths", lambda: [sentinel])
    if guard == "paused":
        sentinel.touch()
        code = "gateway_paused"
    else:
        runner._get_proxy_url = lambda: "http://proxy.invalid"
        code = "proxy_not_supported"
    matrix.set_message_handler(AsyncMock(return_value="must not run"))
    source = SessionSource(
        platform=Platform.MATRIX, chat_id=HELIX_SUPPORT_ROOM_ID, chat_type="group"
    )
    with pytest.raises(ExternalTurnUnavailable) as rejected:
        await runner.process_external_turn(
            "new", MessageEvent(text="hello", source=source)
        )
    assert rejected.value.code == code
    matrix._client.send_message_event.assert_not_awaited()
    # Simulate a turn admitted before the pause/proxy change but still queued.
    completion = asyncio.get_running_loop().create_future()
    runner._external_turn_futures = {"queued": completion}
    event = MessageEvent(
        text="Andrew (spoken): hello",
        source=source,
        internal=True,
        metadata={"external_turn_request_id": "queued"},
    )
    try:
        await matrix.handle_message(event)
        with pytest.raises(ExternalTurnUnavailable) as queued:
            await asyncio.wait_for(completion, timeout=2)
        assert queued.value.code == code
        matrix._message_handler.assert_not_awaited()
        assert not [
            call
            for call in matrix._client.send_message_event.await_args_list
            if "body" in call.args[2]
        ]
    finally:
        await matrix.cancel_background_tasks()
