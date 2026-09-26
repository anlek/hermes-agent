import asyncio
import base64
import io
import json
import wave
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.session import SessionSource, build_session_key
from gateway.trusted_matrix_turn import (
    ANDREW_MATRIX_USER_ID,
    HELIX_SUPPORT_ROOM_ID,
    ExternalTurnResult,
    ExternalTurnUnavailable,
    TrustedMatrixTurnIngress,
)


INGRESS_KEY = "dedicated-test-key-not-the-api-key"
VOICE_MEMOS_ROOM_ID = "!VpwfIwkjDDRdJgKLeF:eightstory.com"
DOMAINLESS_ROOM_ID = "!Nhcu5BS-UMnFX7hBVfVSoXiD7OgH6iRT-xyIuqDnpYQ"
ALLOWED_ROOM_ENV = "MATRIX_PLATFORM_TURN_ALLOWED_ROOM_ID"


def _request(*, include_room_id=True, **overrides):
    body = {
        "request_id": "shortcut-001",
        "text": "What changed today?",
        "tts": False,
    }
    if include_room_id:
        body["room_id"] = HELIX_SUPPORT_ROOM_ID
    body.update(overrides)
    return json.dumps(body).encode()


def _wav_bytes() -> bytes:
    output = io.BytesIO()
    with wave.open(output, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(24_000)
        wav.writeframes(b"\x00\x00" * 32)
    return output.getvalue()


@pytest.mark.asyncio
async def test_matrix_media_fails_closed_when_encryption_state_lookup_fails():
    """A transient state-store failure must never upload plaintext bytes."""
    from plugins.platforms.matrix.adapter import MatrixAdapter

    adapter = object.__new__(MatrixAdapter)
    adapter._encryption = True
    adapter._max_media_bytes = 1024
    client = MagicMock()
    client.crypto = SimpleNamespace()
    client.state_store = MagicMock()
    client.state_store.is_encrypted = AsyncMock(
        side_effect=RuntimeError("transient state-store failure")
    )
    client.crypto.state_store = client.state_store
    client.upload_media = AsyncMock(return_value="mxc://example.org/plaintext")
    client.send_message_event = AsyncMock(return_value="$event")
    adapter._client = client

    result = await adapter._upload_and_send(
        VOICE_MEMOS_ROOM_ID,
        b"private attachment bytes",
        "private.txt",
        "text/plain",
        "m.file",
        metadata={"external_turn_request_id": "trusted-media"},
    )

    assert result.success is False
    assert result.error == "room_encryption_state_unavailable"
    client.upload_media.assert_not_awaited()
    client.send_message_event.assert_not_awaited()


class _FakeRunner:
    def __init__(self):
        self.calls = []
        self.delay: float = 0
        self.deltas = ["Exact ", "Matrix ", "answer"]
        self.delta_callbacks = []

    async def process_external_turn(self, request_id, event, on_delta=None):
        self.calls.append((request_id, event))
        self.delta_callbacks.append(on_delta)
        if self.delay:
            await asyncio.sleep(self.delay)
        if on_delta is not None:
            for delta in self.deltas:
                on_delta(delta)
        return ExternalTurnResult(
            answer="Exact Matrix answer",
            input_event_id="$input",
            reply_event_id="$reply",
        )


@pytest.mark.asyncio
async def test_ingress_rejects_unconfigured_or_wrong_dedicated_key_without_work():
    runner = _FakeRunner()
    disabled = TrustedMatrixTurnIngress(lambda: runner, ingress_key="", api_server_key="api-key")
    status, body = await disabled.handle("Bearer api-key", _request())
    assert (status, body["error"]["code"]) == (404, "not_found")

    ingress = TrustedMatrixTurnIngress(lambda: runner, ingress_key=INGRESS_KEY, api_server_key="api-key")
    status, body = await ingress.handle("Bearer api-key", _request())
    assert (status, body["error"]["code"]) == (401, "unauthorized")
    assert runner.calls == []
    assert INGRESS_KEY not in json.dumps(body)


@pytest.mark.asyncio
async def test_ingress_validates_before_matrix_side_effects():
    runner = _FakeRunner()
    ingress = TrustedMatrixTurnIngress(lambda: runner, ingress_key=INGRESS_KEY, api_server_key="api-key")

    invalid_bodies = [
        (b"{", 400, "invalid_json"),
        (_request(room_id="!other:example.com"), 403, "room_not_allowed"),
        (_request(text=""), 422, "invalid_text"),
        (_request(text="x" * 4001), 413, "text_too_long"),
        (_request(request_id="bad request id"), 422, "invalid_request_id"),
        (_request(tts="yes"), 422, "invalid_tts"),
        (_request(stream="yes"), 422, "invalid_stream"),
        (_request(audio_format="ogg"), 422, "unsupported_audio_format"),
        (_request(extra=True), 422, "unknown_field"),
        (b"{" + b" " * 70_000 + b"}", 413, "body_too_large"),
    ]
    for raw, expected_status, expected_code in invalid_bodies:
        status, body = await ingress.handle(f"Bearer {INGRESS_KEY}", raw)
        assert (status, body["error"]["code"]) == (expected_status, expected_code)
    assert runner.calls == []


@pytest.mark.asyncio
async def test_ingress_defaults_omitted_room_id_to_configured_room():
    runner = _FakeRunner()
    ingress = TrustedMatrixTurnIngress(
        lambda: runner,
        ingress_key=INGRESS_KEY,
        api_server_key="api-key",
        allowed_room_id=VOICE_MEMOS_ROOM_ID,
    )

    status, _body = await ingress.handle(
        f"Bearer {INGRESS_KEY}",
        _request(include_room_id=False),
    )

    assert status == 200
    assert len(runner.calls) == 1
    assert runner.calls[0][1].source.chat_id == VOICE_MEMOS_ROOM_ID


@pytest.mark.asyncio
async def test_ingress_rejects_wrong_room_before_side_effects_with_configured_override():
    runner = _FakeRunner()
    ingress = TrustedMatrixTurnIngress(
        lambda: runner,
        ingress_key=INGRESS_KEY,
        api_server_key="api-key",
        allowed_room_id=VOICE_MEMOS_ROOM_ID,
    )

    status, body = await ingress.handle(f"Bearer {INGRESS_KEY}", _request())

    assert (status, body["error"]["code"]) == (403, "room_not_allowed")
    assert runner.calls == []


def test_allowed_room_config_defaults_overrides_and_does_not_leak(monkeypatch):
    from gateway.config import GatewayConfig
    from gateway.config_env import _apply_env_overrides

    monkeypatch.delenv(ALLOWED_ROOM_ENV, raising=False)
    default_config = GatewayConfig()
    _apply_env_overrides(default_config)
    assert default_config.matrix_platform_turn_allowed_room_id == HELIX_SUPPORT_ROOM_ID

    monkeypatch.setenv(ALLOWED_ROOM_ENV, VOICE_MEMOS_ROOM_ID)
    override_config = GatewayConfig()
    _apply_env_overrides(override_config)
    assert override_config.matrix_platform_turn_allowed_room_id == VOICE_MEMOS_ROOM_ID

    monkeypatch.delenv(ALLOWED_ROOM_ENV)
    fresh_config = GatewayConfig()
    _apply_env_overrides(fresh_config)
    assert fresh_config.matrix_platform_turn_allowed_room_id == HELIX_SUPPORT_ROOM_ID

    yaml_config = GatewayConfig.from_dict(
        {"matrix_platform_turn_allowed_room_id": VOICE_MEMOS_ROOM_ID}
    )
    assert yaml_config.matrix_platform_turn_allowed_room_id == VOICE_MEMOS_ROOM_ID


def test_gateway_config_loads_nested_allowed_room_and_env_wins(
    monkeypatch,
    tmp_path,
):
    from gateway.config import load_gateway_config

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.delenv(ALLOWED_ROOM_ENV, raising=False)
    (tmp_path / "config.yaml").write_text(
        "gateway:\n"
        "  api_server:\n"
        f"    matrix_platform_turn_allowed_room_id: '{VOICE_MEMOS_ROOM_ID}'\n",
        encoding="utf-8",
    )

    yaml_config = load_gateway_config()
    assert yaml_config.matrix_platform_turn_allowed_room_id == VOICE_MEMOS_ROOM_ID

    monkeypatch.setenv(ALLOWED_ROOM_ENV, HELIX_SUPPORT_ROOM_ID)
    env_config = load_gateway_config()
    assert env_config.matrix_platform_turn_allowed_room_id == HELIX_SUPPORT_ROOM_ID


def test_runner_wires_one_resolved_room_to_api_ingress(monkeypatch):
    from gateway.config import GatewayConfig
    from gateway.platforms.api_server import APIServerAdapter
    from gateway.run import GatewayRunner

    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    config = GatewayConfig(
        matrix_platform_turn_allowed_room_id=VOICE_MEMOS_ROOM_ID,
    )
    runner = object.__new__(GatewayRunner)
    runner.config = config

    adapter = runner._create_adapter(
        Platform.API_SERVER,
        PlatformConfig(
            enabled=True,
            extra={"key": "api-key-that-is-long-enough"},
        ),
    )

    assert isinstance(adapter, APIServerAdapter)
    assert (
        adapter._trusted_matrix_turn_ingress._allowed_room_id
        == runner.config.matrix_platform_turn_allowed_room_id
        == VOICE_MEMOS_ROOM_ID
    )


@pytest.mark.parametrize(
    "malformed",
    [
        "",
        "   ",
        "not-a-room-id",
        "!missing-server",
        "!:example.com",
        "!missing-server:",
        "!room:https://example.com",
        "!room:example.com/path",
        "!room:@",
        "!" + ("a" * 250) + ":example.com",
        f"{HELIX_SUPPORT_ROOM_ID},{VOICE_MEMOS_ROOM_ID}",
    ],
)
def test_present_invalid_allowed_room_fails_closed(monkeypatch, malformed):
    from gateway.config import GatewayConfig
    from gateway.config_env import _apply_env_overrides

    monkeypatch.setenv(ALLOWED_ROOM_ENV, malformed)
    config = GatewayConfig.from_dict(
        {"matrix_platform_turn_allowed_room_id": VOICE_MEMOS_ROOM_ID}
    )
    _apply_env_overrides(config)

    assert config.matrix_platform_turn_allowed_room_id is None
    ingress = TrustedMatrixTurnIngress(
        lambda: _FakeRunner(),
        ingress_key=INGRESS_KEY,
        api_server_key="api-key",
        allowed_room_id=config.matrix_platform_turn_allowed_room_id,
    )
    assert ingress.enabled is False


def test_nul_allowed_room_fails_closed():
    from gateway.config import GatewayConfig

    config = GatewayConfig(
        matrix_platform_turn_allowed_room_id="!room:example.com\x00",
    )

    assert config.matrix_platform_turn_allowed_room_id is None


def test_current_matrix_domainless_room_id_is_allowed():
    from gateway.config import GatewayConfig

    config = GatewayConfig(
        matrix_platform_turn_allowed_room_id=DOMAINLESS_ROOM_ID,
    )

    assert config.matrix_platform_turn_allowed_room_id == DOMAINLESS_ROOM_ID


@pytest.mark.asyncio
async def test_ingress_returns_exact_matrix_answer_and_replays_duplicate_request_id():
    runner = _FakeRunner()
    ingress = TrustedMatrixTurnIngress(lambda: runner, ingress_key=INGRESS_KEY, api_server_key="api-key")

    first_status, first = await ingress.handle(f"Bearer {INGRESS_KEY}", _request())
    replay_status, replay = await ingress.handle(f"Bearer {INGRESS_KEY}", _request())

    assert first_status == replay_status == 200
    assert replay == first
    assert first["answer"] == "Exact Matrix answer"
    assert first["matrix"] == {"input_event_id": "$input", "reply_event_id": "$reply"}
    assert first["audio"] is None
    assert len(runner.calls) == 1

    status, conflict = await ingress.handle(
        f"Bearer {INGRESS_KEY}", _request(text="different payload")
    )
    assert (status, conflict["error"]["code"]) == (409, "idempotency_conflict")
    assert len(runner.calls) == 1


@pytest.mark.asyncio
async def test_ingress_stream_emits_provisional_deltas_and_authoritative_replace():
    runner = _FakeRunner()
    runner.deltas = ["I will inspect that first."]
    ingress = TrustedMatrixTurnIngress(
        lambda: runner,
        ingress_key=INGRESS_KEY,
        api_server_key="api-key",
    )
    stream_events = []

    status, body = await ingress.handle(
        f"Bearer {INGRESS_KEY}",
        _request(stream=True),
        on_stream_event=lambda name, data: stream_events.append((name, data)),
    )

    assert status == 200
    assert stream_events == [
        ("delta", "I will inspect that first."),
        ("replace", "Exact Matrix answer"),
    ]
    assert body["answer"] == "Exact Matrix answer"
    assert callable(runner.delta_callbacks[0])


@pytest.mark.asyncio
async def test_stream_representation_is_not_part_of_turn_idempotency():
    runner = _FakeRunner()
    ingress = TrustedMatrixTurnIngress(
        lambda: runner,
        ingress_key=INGRESS_KEY,
        api_server_key="api-key",
    )

    omitted = await ingress.handle(
        f"Bearer {INGRESS_KEY}",
        _request(request_id="same-transport"),
    )
    stream_events = []
    streamed_replay = await ingress.handle(
        f"Bearer {INGRESS_KEY}",
        _request(request_id="same-transport", stream=True),
        on_stream_event=lambda name, data: stream_events.append((name, data)),
    )

    assert streamed_replay == omitted
    assert stream_events == []
    assert len(runner.calls) == 1
    assert runner.delta_callbacks == [None]


@pytest.mark.asyncio
async def test_completed_stream_replay_reuses_recorded_events_without_second_turn():
    runner = _FakeRunner()
    ingress = TrustedMatrixTurnIngress(
        lambda: runner,
        ingress_key=INGRESS_KEY,
        api_server_key="api-key",
    )
    first_events = []
    replay_events = []

    first = await ingress.handle(
        f"Bearer {INGRESS_KEY}",
        _request(stream=True),
        on_stream_event=lambda name, data: first_events.append((name, data)),
    )
    replay = await ingress.handle(
        f"Bearer {INGRESS_KEY}",
        _request(stream=True),
        on_stream_event=lambda name, data: replay_events.append((name, data)),
    )

    assert replay == first
    assert first_events == replay_events == [
        ("delta", delta) for delta in runner.deltas
    ]
    assert len(runner.calls) == 1


@pytest.mark.asyncio
async def test_concurrent_stream_subscribers_share_native_turn_and_event_history():
    first_delta_sent = asyncio.Event()
    release = asyncio.Event()

    class ControlledRunner(_FakeRunner):
        async def process_external_turn(self, request_id, event, on_delta=None):
            self.calls.append((request_id, event))
            self.delta_callbacks.append(on_delta)
            assert on_delta is not None
            on_delta("Exact ")
            first_delta_sent.set()
            await release.wait()
            on_delta("Matrix answer")
            return ExternalTurnResult("Exact Matrix answer", "$input", "$reply")

    runner = ControlledRunner()
    ingress = TrustedMatrixTurnIngress(
        lambda: runner,
        ingress_key=INGRESS_KEY,
        api_server_key="api-key",
    )
    first_events = []
    second_events = []
    first = asyncio.create_task(
        ingress.handle(
            f"Bearer {INGRESS_KEY}",
            _request(stream=True),
            on_stream_event=lambda name, data: first_events.append((name, data)),
        )
    )
    await asyncio.wait_for(first_delta_sent.wait(), timeout=1)
    second = asyncio.create_task(
        ingress.handle(
            f"Bearer {INGRESS_KEY}",
            _request(stream=True),
            on_stream_event=lambda name, data: second_events.append((name, data)),
        )
    )
    await asyncio.sleep(0)
    release.set()

    first_result, second_result = await asyncio.gather(first, second)

    assert first_result == second_result
    assert first_events == second_events == [
        ("delta", "Exact "),
        ("delta", "Matrix answer"),
    ]
    assert len(runner.calls) == 1


@pytest.mark.asyncio
async def test_durable_request_tombstone_prevents_duplicate_after_ingress_restart(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    runner = _FakeRunner()
    first_ingress = TrustedMatrixTurnIngress(
        lambda: runner, ingress_key=INGRESS_KEY, api_server_key="api-key"
    )
    first_status, _first = await first_ingress.handle(
        f"Bearer {INGRESS_KEY}", _request()
    )

    restarted_ingress = TrustedMatrixTurnIngress(
        lambda: runner, ingress_key=INGRESS_KEY, api_server_key="api-key"
    )
    replay_status, replay = await restarted_ingress.handle(
        f"Bearer {INGRESS_KEY}", _request()
    )

    assert first_status == 200
    assert replay_status == 409
    assert replay["error"]["code"] == "idempotency_replay_unavailable"
    conflict_status, conflict = await restarted_ingress.handle(
        f"Bearer {INGRESS_KEY}", _request(text="different after restart")
    )
    assert conflict_status == 409
    assert conflict["error"]["code"] == "idempotency_conflict"
    assert len(runner.calls) == 1


@pytest.mark.asyncio
async def test_replay_cache_bounds_large_audio_without_repeating_matrix_turn():
    from gateway.trusted_matrix_turn import _TurnCache

    cache = _TurnCache(max_response_bytes=256)
    calls = 0

    async def compute():
        nonlocal calls
        calls += 1
        return 200, {
            "ok": True,
            "answer": "done",
            "audio": {"data_base64": "A" * 2048},
        }

    first = await cache.get_or_compute("request-1", "fingerprint", compute)
    replay = await cache.get_or_compute("request-1", "fingerprint", compute)

    assert first[0] == 200
    assert replay[0] == 409
    assert replay[1]["error"]["code"] == "idempotency_response_evicted"
    assert calls == 1


@pytest.mark.asyncio
async def test_ingress_coalesces_concurrent_duplicates_and_cleans_inflight_registry():
    runner = _FakeRunner()
    runner.delay = 0.02
    ingress = TrustedMatrixTurnIngress(lambda: runner, ingress_key=INGRESS_KEY, api_server_key="api-key")

    results = await asyncio.gather(
        ingress.handle(f"Bearer {INGRESS_KEY}", _request()),
        ingress.handle(f"Bearer {INGRESS_KEY}", _request()),
    )
    assert results[0] == results[1]
    assert len(runner.calls) == 1
    assert ingress.inflight_count == 0


@pytest.mark.asyncio
async def test_ingress_timeout_is_bounded_cached_and_cleans_inflight_registry():
    runner = _FakeRunner()
    runner.delay = 1
    ingress = TrustedMatrixTurnIngress(
        lambda: runner,
        ingress_key=INGRESS_KEY,
        api_server_key="api-key",
        timeout_seconds=0.01,
    )

    status, body = await ingress.handle(f"Bearer {INGRESS_KEY}", _request())
    assert (status, body["error"]["code"]) == (504, "turn_timeout")
    assert ingress.inflight_count == 0

    replay_status, replay = await ingress.handle(f"Bearer {INGRESS_KEY}", _request())
    assert (replay_status, replay) == (status, body)
    assert len(runner.calls) == 1


@pytest.mark.asyncio
async def test_ingress_timeout_covers_tts_synthesis():
    runner = _FakeRunner()

    async def slow_synthesize(_text):
        await asyncio.sleep(1)
        return _wav_bytes()

    ingress = TrustedMatrixTurnIngress(
        lambda: runner,
        ingress_key=INGRESS_KEY,
        api_server_key="api-key",
        timeout_seconds=0.01,
        wav_synthesizer=slow_synthesize,
    )

    status, body = await ingress.handle(
        f"Bearer {INGRESS_KEY}", _request(tts=True)
    )
    assert (status, body["error"]["code"]) == (504, "turn_timeout")
    assert ingress.inflight_count == 0


@pytest.mark.asyncio
async def test_tts_false_does_not_synthesize_and_tts_wav_uses_exact_answer():
    runner = _FakeRunner()
    calls = []

    async def synthesize(text):
        calls.append(text)
        return _wav_bytes()

    ingress = TrustedMatrixTurnIngress(
        lambda: runner,
        ingress_key=INGRESS_KEY,
        api_server_key="api-key",
        wav_synthesizer=synthesize,
    )
    status, no_audio = await ingress.handle(f"Bearer {INGRESS_KEY}", _request())
    assert status == 200
    assert no_audio["audio"] is None
    assert calls == []

    status, with_audio = await ingress.handle(
        f"Bearer {INGRESS_KEY}", _request(request_id="shortcut-tts", tts=True)
    )
    assert status == 200
    assert calls == ["Exact Matrix answer"]
    audio = with_audio["audio"]
    assert audio["format"] == "wav"
    assert audio["mime_type"] == "audio/wav"
    decoded = base64.b64decode(audio["data_base64"], validate=True)
    with wave.open(io.BytesIO(decoded), "rb") as wav:
        assert wav.getnframes() > 0


@pytest.mark.asyncio
async def test_local_kokoro_wav_requires_exact_voice_model_and_wav_config(
    monkeypatch, tmp_path
):
    from gateway.trusted_matrix_turn import (
        ExternalTurnUnavailable,
        synthesize_local_kokoro_wav,
    )

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(
        "hermes_cli.config.load_config",
        lambda: {
            "tts": {
                "providers": {
                    "local-kokoro": {
                        "type": "command",
                        "command": "kokoro {input_path} {output_path}",
                        "voice": "wrong-voice",
                        "model": "mlx-community/Kokoro-82M-bf16",
                        "output_format": "wav",
                    }
                }
            }
        },
    )
    synth = MagicMock(
        return_value=json.dumps({"success": True, "file_path": "unused"})
    )
    monkeypatch.setattr("tools.tts_tool.text_to_speech_tool", synth)

    with pytest.raises(ExternalTurnUnavailable) as exc:
        await synthesize_local_kokoro_wav("Exact Matrix answer")
    assert exc.value.code == "tts_misconfigured"
    synth.assert_not_called()


@pytest.mark.asyncio
async def test_local_kokoro_wav_uses_approved_config_and_exact_answer(
    monkeypatch, tmp_path
):
    from gateway.trusted_matrix_turn import synthesize_local_kokoro_wav

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(
        "hermes_cli.config.load_config",
        lambda: {
            "tts": {
                "providers": {
                    "local-kokoro": {
                        "type": "command",
                        "command": (
                            "kokoro --voice {voice} --model {model} "
                            "--format {format} {input_path} {output_path}"
                        ),
                        "voice": "af_jessica",
                        "model": "mlx-community/Kokoro-82M-bf16",
                        "output_format": "ogg",
                    }
                }
            }
        },
    )
    calls = []

    def synth(
        text,
        output_path,
        provider_name,
        provider_config,
        tts_config,
        cancel_event,
    ):
        calls.append(
            {
                "text": text,
                "output_path": output_path,
                "provider_name": provider_name,
                "provider_config": provider_config,
                "tts_config": tts_config,
                "cancel_event": cancel_event,
            }
        )
        Path(output_path).write_bytes(_wav_bytes())
        return output_path

    monkeypatch.setattr("tools.tts_command_provider._generate_command_tts", synth)
    answer = "Exact Matrix answer — punctuation stays here."
    wav_data = await synthesize_local_kokoro_wav(answer)

    assert wav_data == _wav_bytes()
    assert calls[0]["text"] == answer
    assert calls[0]["provider_name"] == "local-kokoro"
    assert calls[0]["provider_config"]["voice"] == "af_jessica"
    assert calls[0]["provider_config"]["model"] == "mlx-community/Kokoro-82M-bf16"
    assert calls[0]["provider_config"]["format"] == "wav"
    assert calls[0]["cancel_event"].is_set() is False
    assert not Path(calls[0]["output_path"]).exists()


@pytest.mark.asyncio
async def test_runner_external_turn_uses_live_matrix_adapter_and_completion_future():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner._external_turn_futures = {}

    class MatrixAdapter:
        _message_handler = staticmethod(lambda _event: None)

        def __init__(self):
            self.sent = []
            self.events = []
            self.gateway_runner = runner

        async def trusted_turn_readiness(self, room_id):
            assert room_id == HELIX_SUPPORT_ROOM_ID
            return True, None

        def build_source(self, **kwargs):
            return SessionSource(platform=Platform.MATRIX, **kwargs)

        async def build_trusted_turn_source(
            self, *, room_id, sender, event_id, user_name
        ):
            return SessionSource(
                platform=Platform.MATRIX,
                chat_id=room_id,
                chat_type="group",
                user_id=sender,
                user_name=user_name,
                message_id=event_id,
            )

        async def send(self, chat_id, content, **kwargs):
            self.sent.append((chat_id, content, kwargs))
            return SendResult(success=True, message_id="$input")

        async def handle_message(self, event):
            event._gateway_accepted = True
            self.events.append(event)
            assert event.metadata["external_turn_force_non_streaming"] is True
            callback = getattr(
                event.source,
                "_external_turn_stream_delta_callback",
                None,
            )
            assert callable(callback)
            callback("Exact ")
            callback("Matrix answer")
            runner._resolve_external_turn(
                event,
                answer="Exact Matrix answer",
                reply_event_id="$reply",
            )

    adapter = MatrixAdapter()
    runner.adapters = {Platform.MATRIX: adapter}
    event = MessageEvent(
        text="What changed today?",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
            user_name="Andrew",
        ),
    )

    deltas = []
    result = await runner.process_external_turn(
        "shortcut-001",
        event,
        on_delta=deltas.append,
    )

    assert result == ExternalTurnResult("Exact Matrix answer", "$input", "$reply")
    assert adapter.sent[0][1] == "Andrew (spoken): What changed today?"
    assert adapter.events[0].text == "Andrew (spoken): What changed today?"
    assert adapter.events[0].message_id == "$input"
    assert adapter.events[0].source.message_id == "$input"
    assert adapter.events[0].source.user_id == ANDREW_MATRIX_USER_ID
    assert adapter.events[0].source.profile == "default"
    native_source = SessionSource(
        platform=Platform.MATRIX,
        chat_id=HELIX_SUPPORT_ROOM_ID,
        chat_type="group",
        user_id=ANDREW_MATRIX_USER_ID,
    )
    for group_sessions_per_user in (False, True):
        assert build_session_key(
            adapter.events[0].source,
            group_sessions_per_user=group_sessions_per_user,
        ) == build_session_key(
            native_source,
            group_sessions_per_user=group_sessions_per_user,
        )
    from gateway.run import _platform_config_key

    assert _platform_config_key(adapter.events[0].source.platform) == "matrix"
    assert adapter.events[0].metadata["external_turn_request_id"] == "shortcut-001"
    assert deltas == ["Exact ", "Matrix answer"]
    assert "_external_turn_stream_delta_callback" not in adapter.events[0].source.to_dict()
    assert runner._external_turn_futures == {}


@pytest.mark.asyncio
async def test_matrix_trusted_source_uses_native_room_identity_and_thread_policy(
    monkeypatch,
):
    from plugins.platforms.matrix.adapter import MatrixAdapter

    adapter = object.__new__(MatrixAdapter)
    adapter.platform = Platform.MATRIX
    adapter.gateway_runner = None
    adapter._allowed_rooms = set()
    adapter._free_rooms = set()
    adapter._require_mention = False
    adapter._thread_require_mention = False
    adapter._dm_mention_threads = False
    adapter._dm_auto_thread = True
    adapter._matrix_session_scope = "auto"
    adapter._auto_thread = True
    adapter._threads = SimpleNamespace(
        mark=MagicMock(),
        __contains__=lambda _self, _item: False,
    )
    identity = SimpleNamespace(
        display_name="Voice Memos",
        room_topic="Private voice turns",
        server_name="eightstory.com",
    )
    monkeypatch.setattr(
        adapter,
        "_resolve_room_identity",
        AsyncMock(return_value=identity),
    )
    monkeypatch.setattr(adapter, "_is_dm_room", AsyncMock(return_value=False))
    monkeypatch.setattr(adapter, "_get_display_name", AsyncMock(return_value="Andrew"))
    monkeypatch.setattr(adapter, "_is_bot_mentioned", lambda *_args: False)
    monkeypatch.setattr(adapter, "_background_read_receipt", MagicMock())

    native = await adapter._resolve_message_context(
        VOICE_MEMOS_ROOM_ID,
        ANDREW_MATRIX_USER_ID,
        "$input",
        "Andrew (spoken): hello",
        {"msgtype": "m.text", "body": "Andrew (spoken): hello"},
        {},
    )
    assert native is not None
    native_source = native[-1]
    preflight_source = await adapter.build_trusted_turn_preflight_source(
        room_id=VOICE_MEMOS_ROOM_ID,
        sender=ANDREW_MATRIX_USER_ID,
        user_name="Andrew",
    )
    trusted_source = await adapter.build_trusted_turn_source(
        room_id=VOICE_MEMOS_ROOM_ID,
        sender=ANDREW_MATRIX_USER_ID,
        event_id="$input",
        user_name="Andrew",
    )

    assert trusted_source.thread_id == "$input"
    assert trusted_source.message_id == "$input"
    assert trusted_source.chat_name == "Voice Memos"
    assert trusted_source.chat_topic == "Private voice turns"
    assert preflight_source.chat_name == trusted_source.chat_name
    assert preflight_source.chat_topic == trusted_source.chat_topic
    assert preflight_source.guild_id == trusted_source.guild_id
    assert build_session_key(
        trusted_source,
        group_sessions_per_user=True,
        thread_sessions_per_user=False,
    ) == build_session_key(
        native_source,
        group_sessions_per_user=True,
        thread_sessions_per_user=False,
    )


@pytest.mark.asyncio
async def test_runner_external_turn_uses_configured_room_at_native_boundary():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner._external_turn_futures = {}
    runner.config = SimpleNamespace(matrix_platform_turn_allowed_room_id=VOICE_MEMOS_ROOM_ID)

    class MatrixAdapter:
        _message_handler = staticmethod(lambda _event: None)

        def __init__(self):
            self.sent = []

        async def trusted_turn_readiness(self, room_id):
            assert room_id == VOICE_MEMOS_ROOM_ID
            return True, None

        def build_source(self, **kwargs):
            return SessionSource(platform=Platform.MATRIX, **kwargs)

        async def build_trusted_turn_source(
            self, *, room_id, sender, event_id, user_name
        ):
            return SessionSource(
                platform=Platform.MATRIX,
                chat_id=room_id,
                chat_type="group",
                user_id=sender,
                user_name=user_name,
                message_id=event_id,
            )

        async def send(self, chat_id, content, **kwargs):
            self.sent.append((chat_id, content, kwargs))
            return SendResult(success=True, message_id="$input")

        async def handle_message(self, event):
            event._gateway_accepted = True
            runner._resolve_external_turn(
                event,
                answer="Exact Matrix answer",
                reply_event_id="$reply",
            )

    adapter = MatrixAdapter()
    runner.adapters = {Platform.MATRIX: adapter}
    event = MessageEvent(
        text="What changed today?",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=VOICE_MEMOS_ROOM_ID,
            chat_type="group",
        ),
    )

    result = await runner.process_external_turn("shortcut-configured", event)

    assert result.reply_event_id == "$reply"
    assert adapter.sent[0][0] == VOICE_MEMOS_ROOM_ID


@pytest.mark.asyncio
async def test_runner_external_turn_rejects_wrong_room_before_adapter_side_effects():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner._external_turn_futures = {}
    runner.config = SimpleNamespace(matrix_platform_turn_allowed_room_id=VOICE_MEMOS_ROOM_ID)

    class MatrixAdapter:
        async def trusted_turn_readiness(self, _room_id):
            raise AssertionError("readiness must not run for a disallowed room")

    runner.adapters = {Platform.MATRIX: MatrixAdapter()}
    event = MessageEvent(
        text="hello",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
        ),
    )

    with pytest.raises(ExternalTurnUnavailable) as exc:
        await runner.process_external_turn("shortcut-wrong-room", event)

    assert exc.value.code == "room_not_allowed"
    assert runner._external_turn_futures == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("ready_code", ["matrix_disconnected", "e2ee_disabled", "room_unencrypted", "olm_unavailable"])
async def test_runner_external_turn_fails_closed_before_send(ready_code):
    from gateway.trusted_matrix_turn import ExternalTurnUnavailable
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner._external_turn_futures = {}

    class MatrixAdapter:
        sent = False

        async def trusted_turn_readiness(self, room_id):
            return False, ready_code

        async def send(self, *args, **kwargs):
            self.sent = True
            raise AssertionError("send must not run")

    adapter = MatrixAdapter()
    runner.adapters = {Platform.MATRIX: adapter}
    event = MessageEvent(
        text="hello",
        message_type=MessageType.TEXT,
        source=SessionSource(platform=Platform.MATRIX, chat_id=HELIX_SUPPORT_ROOM_ID, chat_type="group"),
    )

    with pytest.raises(ExternalTurnUnavailable) as exc:
        await runner.process_external_turn("shortcut-001", event)
    assert exc.value.code == ready_code
    assert adapter.sent is False
    assert runner._external_turn_futures == {}


@pytest.mark.asyncio
async def test_matrix_readiness_accepts_reconciled_live_device_id():
    from plugins.platforms.matrix.adapter import MatrixAdapter

    adapter = object.__new__(MatrixAdapter)
    adapter._running = True
    adapter._closing = False
    adapter._encryption = True
    adapter._device_id = "stale-configured-device"
    adapter._crypto_db = object()
    state_store = SimpleNamespace(is_encrypted=AsyncMock(return_value=True))
    adapter._client = SimpleNamespace(
        crypto=SimpleNamespace(state_store=state_store),
        device_id="device-from-access-token",
        state_store=state_store,
    )

    assert await adapter.trusted_turn_readiness(HELIX_SUPPORT_ROOM_ID) == (
        True,
        None,
    )
    state_store.is_encrypted.assert_awaited_once()


@pytest.mark.asyncio
async def test_runner_external_turn_cancellation_cleans_completion_future():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner._external_turn_futures = {}
    entered = asyncio.Event()

    class MatrixAdapter:
        _message_handler = staticmethod(lambda _event: None)
        config = SimpleNamespace(
            extra={"group_sessions_per_user": True, "thread_sessions_per_user": False}
        )

        def __init__(self):
            self._pending_messages = {}
            self._active_session_events = {}

        async def trusted_turn_readiness(self, room_id):
            return True, None

        def build_source(self, **kwargs):
            return SessionSource(platform=Platform.MATRIX, **kwargs)

        async def build_trusted_turn_source(
            self, *, room_id, sender, event_id, user_name
        ):
            return SessionSource(
                platform=Platform.MATRIX,
                chat_id=room_id,
                chat_type="group",
                user_id=sender,
                user_name=user_name,
                message_id=event_id,
            )

        async def send(self, *_args, **_kwargs):
            return SendResult(success=True, message_id="$input")

        async def handle_message(self, event):
            event._gateway_accepted = True
            session_key = build_session_key(
                event.source, group_sessions_per_user=True
            )
            self._pending_messages[session_key] = event
            entered.set()

    adapter = MatrixAdapter()
    runner.__dict__["adapters"] = {Platform.MATRIX: adapter}
    runner.__dict__["_peek_session_state"] = lambda _session_key: None
    event = MessageEvent(
        text="hello",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
        ),
    )

    task = asyncio.create_task(runner.process_external_turn("cancel-me", event))
    await asyncio.wait_for(entered.wait(), timeout=1)
    assert set(runner._external_turn_futures) == {"cancel-me"}
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert runner._external_turn_futures == {}
    assert adapter._pending_messages == {}


@pytest.mark.asyncio
async def test_external_turn_timeout_waits_for_delivery_already_in_flight():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner._external_turn_futures = {}

    class MatrixAdapter:
        _message_handler = staticmethod(lambda _event: None)

        def __init__(self):
            self.completion_task = None

        async def trusted_turn_readiness(self, room_id):
            return True, None

        def build_source(self, **kwargs):
            return SessionSource(platform=Platform.MATRIX, **kwargs)

        async def build_trusted_turn_source(
            self, *, room_id, sender, event_id, user_name
        ):
            return SessionSource(
                platform=Platform.MATRIX,
                chat_id=room_id,
                chat_type="group",
                user_id=sender,
                user_name=user_name,
                message_id=event_id,
            )

        async def send(self, *_args, **_kwargs):
            return SendResult(success=True, message_id="$input")

        async def handle_message(self, event):
            event._gateway_accepted = True
            event.metadata["external_turn_delivery_started"] = True

            async def finish_delivery():
                await asyncio.sleep(0.03)
                runner._resolve_external_turn(
                    event,
                    answer="Delivered during grace",
                    reply_event_id="$reply",
                )

            self.completion_task = asyncio.create_task(finish_delivery())

    adapter = MatrixAdapter()
    runner.__dict__["adapters"] = {Platform.MATRIX: adapter}
    event = MessageEvent(
        text="hello",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
        ),
    )

    result = await asyncio.wait_for(
        runner.process_external_turn("delivery-grace", event), timeout=0.01
    )
    assert result == ExternalTurnResult(
        "Delivered during grace", "$input", "$reply"
    )
    assert event.metadata.get("external_turn_cancelled") is not True
    assert adapter.completion_task is not None
    await adapter.completion_task


@pytest.mark.asyncio
async def test_external_turn_timeout_marks_event_before_active_registration():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    event = MessageEvent(
        text="Andrew (spoken): hello",
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
            user_id=ANDREW_MATRIX_USER_ID,
        ),
        internal=True,
        metadata={"external_turn_request_id": "timeout-scheduling-gap"},
    )
    adapter = SimpleNamespace(
        config=SimpleNamespace(
            extra={"group_sessions_per_user": True, "thread_sessions_per_user": False}
        ),
        _pending_messages={},
        _active_session_events={},
        _active_sessions={},
    )
    runner.__dict__["_adapter_for_source"] = lambda _source: adapter
    runner.__dict__["_peek_session_state"] = lambda _key: None

    await runner._cancel_external_turn_work(event)

    assert event.metadata["external_turn_cancelled"] is True


@pytest.mark.asyncio
async def test_active_external_turn_timeout_interrupts_agent_without_releasing_lane():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    event = MessageEvent(
        text="Andrew (spoken): hello",
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
            user_id=ANDREW_MATRIX_USER_ID,
        ),
        internal=True,
        metadata={"external_turn_request_id": "timeout-active"},
    )
    session_key = build_session_key(event.source, group_sessions_per_user=True)
    owner_task = asyncio.create_task(asyncio.Event().wait())
    guard = asyncio.Event()
    adapter = SimpleNamespace(
        config=SimpleNamespace(
            extra={"group_sessions_per_user": True, "thread_sessions_per_user": False}
        ),
        _pending_messages={},
        _active_session_events={session_key: event},
        _active_sessions={session_key: guard},
        _session_tasks={session_key: owner_task},
        cancel_session_processing=AsyncMock(
            side_effect=AssertionError("active executor ownership must be retained")
        ),
    )
    agent = MagicMock()
    state = SimpleNamespace(
        turn=SimpleNamespace(agent=agent),
        conversation=SimpleNamespace(queued_events=[]),
    )
    runner.__dict__["_adapter_for_source"] = lambda _source: adapter
    runner.__dict__["_peek_session_state"] = lambda _key: state

    try:
        await runner._cancel_external_turn_work(event)
        assert event.metadata["external_turn_cancelled"] is True
        agent.interrupt.assert_called_once()
        assert owner_task.cancelled() is False
        assert adapter._active_sessions[session_key] is guard
        assert adapter._session_tasks[session_key] is owner_task
        adapter.cancel_session_processing.assert_not_awaited()
    finally:
        owner_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner_task


@pytest.mark.asyncio
async def test_runner_external_turn_rejects_non_default_profile_route():
    from gateway.run import GatewayRunner

    class MatrixAdapter(BasePlatformAdapter):
        def __init__(self):
            super().__init__(PlatformConfig(enabled=True), Platform.MATRIX)

            async def handler(_event):
                return None

            self._message_handler = handler

        async def connect(self, *, is_reconnect: bool = False) -> bool:
            return True

        async def disconnect(self):
            return None

        async def trusted_turn_readiness(self, room_id):
            return True, None

        async def get_chat_info(self, chat_id):
            return {"id": chat_id}

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            raise AssertionError("profile mismatch must fail before Matrix send")

    runner = object.__new__(GatewayRunner)
    runner.__dict__["_profile_name_for_source"] = lambda _source: "private"
    adapter = MatrixAdapter()
    setattr(adapter, "gateway_runner", runner)
    runner.__dict__["adapters"] = {Platform.MATRIX: adapter}
    event = MessageEvent(
        text="hello",
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
        ),
    )

    with pytest.raises(ExternalTurnUnavailable) as exc_info:
        await runner.process_external_turn("wrong-profile", event)

    assert exc_info.value.code == "profile_scope_mismatch"
    assert runner.__dict__.get("_external_turn_futures", {}) == {}


@pytest.mark.asyncio
async def test_runner_external_turn_rejects_unserved_profile_route_before_send():
    from gateway.profile_routing import ProfileRouteRejected
    from gateway.run import GatewayRunner

    class MatrixAdapter(BasePlatformAdapter):
        def __init__(self):
            super().__init__(PlatformConfig(enabled=True), Platform.MATRIX)
            self.sent = False

            async def handler(_event):
                return None

            self._message_handler = handler

        async def connect(self, *, is_reconnect: bool = False) -> bool:
            return True

        async def disconnect(self):
            return None

        async def trusted_turn_readiness(self, room_id):
            return True, None

        async def build_trusted_turn_source(
            self, *, room_id, sender, event_id, user_name
        ):
            raise AssertionError("unserved route must fail before Matrix send")

        async def get_chat_info(self, chat_id):
            return {"id": chat_id}

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            self.sent = True
            raise AssertionError("unserved route must fail before Matrix send")

    runner = object.__new__(GatewayRunner)

    def reject_route(_source):
        raise ProfileRouteRejected("private")

    runner.__dict__["_profile_name_for_source"] = reject_route
    adapter = MatrixAdapter()
    adapter.gateway_runner = runner
    runner.adapters = {Platform.MATRIX: adapter}
    event = MessageEvent(
        text="hello",
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
        ),
    )

    with pytest.raises(ExternalTurnUnavailable) as exc_info:
        await runner.process_external_turn("unserved-profile", event)

    assert exc_info.value.code == "profile_scope_mismatch"
    assert adapter.sent is False
    assert runner.__dict__.get("_external_turn_futures", {}) == {}


@pytest.mark.asyncio
async def test_busy_trusted_turn_does_not_consume_pending_clarify(monkeypatch):
    from tools import clarify_gateway

    class _Adapter(BasePlatformAdapter):
        async def connect(self, *, is_reconnect=False):
            return True

        async def disconnect(self):
            return None

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            return SendResult(success=True, message_id="$unexpected")

        async def get_chat_info(self, chat_id):
            return {}

    adapter = _Adapter(
        PlatformConfig(enabled=True, typing_indicator=False),
        Platform.MATRIX,
    )
    adapter._message_handler = AsyncMock(return_value="clarify consumed")
    adapter._send_with_retry = AsyncMock(
        return_value=SendResult(success=True, message_id="$unexpected")
    )
    adapter._busy_session_handler = AsyncMock(return_value=True)
    event = MessageEvent(
        text="Andrew (spoken): queued turn",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=VOICE_MEMOS_ROOM_ID,
            chat_type="group",
            user_id=ANDREW_MATRIX_USER_ID,
        ),
        internal=True,
        metadata={"external_turn_request_id": "clarify-safe-turn"},
    )
    session_key = build_session_key(
        event.source,
        group_sessions_per_user=True,
        thread_sessions_per_user=False,
    )
    owner_release = asyncio.Event()
    owner_task = asyncio.create_task(owner_release.wait())
    adapter._active_sessions[session_key] = asyncio.Event()
    adapter._session_tasks[session_key] = owner_task
    monkeypatch.setattr(
        clarify_gateway,
        "get_pending_for_session",
        lambda *_args, **_kwargs: object(),
    )

    try:
        await adapter.handle_message(event)
    finally:
        owner_release.set()
        await owner_task

    adapter._busy_session_handler.assert_awaited_once_with(event, session_key)
    adapter._message_handler.assert_not_awaited()
    adapter._send_with_retry.assert_not_awaited()


@pytest.mark.asyncio
async def test_busy_trusted_turns_queue_as_distinct_fifo_session_turns():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    adapter = SimpleNamespace(_pending_messages={})
    queued_events = []
    state = SimpleNamespace(
        conversation=SimpleNamespace(queued_events=queued_events)
    )
    runner.__dict__["_adapter_for_source"] = lambda source: adapter
    runner.__dict__["_peek_session_state"] = lambda session_key: state
    runner.__dict__["_session_state"] = lambda session_key: state
    resolve_external_turn = MagicMock()
    is_user_authorized = MagicMock(return_value=True)
    runner.__dict__["_resolve_external_turn"] = resolve_external_turn
    runner.__dict__["_is_user_authorized"] = is_user_authorized

    def turn(request_id, text):
        source = SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
            user_id=ANDREW_MATRIX_USER_ID,
        )
        setattr(source, "_external_turn_force_non_streaming", True)
        return MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            internal=True,
            metadata={"external_turn_request_id": request_id},
        )

    first = turn("request-1", "first")
    second = turn("request-2", "second")
    session_key = build_session_key(first.source)

    assert await runner._handle_active_session_busy_message(first, session_key)
    assert await runner._handle_active_session_busy_message(second, session_key)
    assert adapter._pending_messages[session_key] is first
    assert queued_events == [second]
    resolve_external_turn.assert_not_called()
    is_user_authorized.assert_not_called()


@pytest.mark.asyncio
async def test_concurrent_trusted_turns_execute_sequentially_with_exact_event_ids():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner._external_turn_futures = {}
    queued_events = []
    state = SimpleNamespace(
        conversation=SimpleNamespace(queued_events=queued_events)
    )
    runner.__dict__["_peek_session_state"] = lambda _key: state
    runner.__dict__["_session_state"] = lambda _key: state
    runner.__dict__["_profile_name_for_source"] = lambda _source: None

    class Adapter(BasePlatformAdapter):
        def __init__(self):
            super().__init__(
                PlatformConfig(
                    enabled=True,
                    extra={
                        "group_sessions_per_user": True,
                        "thread_sessions_per_user": False,
                    },
                ),
                Platform.MATRIX,
            )
            self.sent = []

        async def connect(self, *, is_reconnect: bool = False) -> bool:
            return True

        async def disconnect(self):
            return None

        async def trusted_turn_readiness(self, _room_id):
            return True, None

        async def build_trusted_turn_source(
            self, *, room_id, sender, event_id, user_name
        ):
            return self.build_source(
                chat_id=room_id,
                chat_type="group",
                user_id=sender,
                user_name=user_name,
                message_id=event_id,
            )

        async def get_chat_info(self, chat_id):
            return {"id": chat_id}

        async def send(
            self,
            chat_id,
            content,
            reply_to=None,
            metadata=None,
        ):
            event_id = f"$event-{len(self.sent) + 1}"
            self.sent.append((event_id, chat_id, content, reply_to, metadata))
            return SendResult(success=True, message_id=event_id)

    adapter = Adapter()
    setattr(adapter, "gateway_runner", runner)
    runner.__dict__["adapters"] = {Platform.MATRIX: adapter}
    runner.__dict__["_adapter_for_source"] = lambda _source: adapter
    adapter._busy_session_handler = runner._handle_active_session_busy_message

    entered_first = asyncio.Event()
    release_first = asyncio.Event()
    order = []
    concurrency = 0
    max_concurrency = 0

    async def handler(event):
        nonlocal concurrency, max_concurrency
        concurrency += 1
        max_concurrency = max(max_concurrency, concurrency)
        order.append(event.text)
        if len(order) == 1:
            entered_first.set()
            await release_first.wait()
        await asyncio.sleep(0)
        concurrency -= 1
        return f"answer:{event.text}"

    adapter._message_handler = handler

    def event(text):
        return MessageEvent(
            text=text,
            source=SessionSource(
                platform=Platform.MATRIX,
                chat_id=HELIX_SUPPORT_ROOM_ID,
                chat_type="group",
            ),
        )

    first_task = asyncio.create_task(
        runner.process_external_turn("sequential-1", event("first"))
    )
    await asyncio.wait_for(entered_first.wait(), timeout=2)
    second_task = asyncio.create_task(
        runner.process_external_turn("sequential-2", event("second"))
    )
    await asyncio.sleep(0.05)
    assert max_concurrency == 1
    release_first.set()

    first, second = await asyncio.wait_for(
        asyncio.gather(first_task, second_task), timeout=3
    )
    await adapter.cancel_background_tasks()

    assert max_concurrency == 1
    assert order == ["Andrew (spoken): first", "Andrew (spoken): second"]
    assert first.input_event_id == "$event-1"
    assert first.reply_event_id == "$event-3"
    assert second.input_event_id == "$event-2"
    assert second.reply_event_id == "$event-4"
    assert first.answer == "answer:Andrew (spoken): first"
    assert second.answer == "answer:Andrew (spoken): second"


@pytest.mark.asyncio
async def test_ingress_rate_limit_is_bounded_and_replay_does_not_consume_it():
    runner = _FakeRunner()
    ingress = TrustedMatrixTurnIngress(
        lambda: runner,
        ingress_key=INGRESS_KEY,
        api_server_key="api-key",
        rate_limit_requests=1,
        rate_limit_window_seconds=60,
    )
    assert (await ingress.handle(f"Bearer {INGRESS_KEY}", _request()))[0] == 200
    assert (await ingress.handle(f"Bearer {INGRESS_KEY}", _request()))[0] == 200
    status, body = await ingress.handle(
        f"Bearer {INGRESS_KEY}", _request(request_id="shortcut-002")
    )
    assert (status, body["error"]["code"]) == (429, "rate_limited")
    assert len(runner.calls) == 1


def test_durable_idempotency_capacity_covers_the_full_retention_window():
    from gateway.trusted_matrix_turn import (
        DEFAULT_RATE_LIMIT_REQUESTS,
        DEFAULT_RATE_LIMIT_WINDOW_SECONDS,
        IDEMPOTENCY_MAX_TOMBSTONES,
        IDEMPOTENCY_RETENTION_SECONDS,
    )

    windows = int(
        (IDEMPOTENCY_RETENTION_SECONDS + DEFAULT_RATE_LIMIT_WINDOW_SECONDS - 1)
        // DEFAULT_RATE_LIMIT_WINDOW_SECONDS
    )
    assert IDEMPOTENCY_MAX_TOMBSTONES >= windows * DEFAULT_RATE_LIMIT_REQUESTS


@pytest.mark.asyncio
async def test_api_server_registers_route_only_with_distinct_dedicated_key(monkeypatch):
    from gateway.platforms.api_server import APIServerAdapter

    monkeypatch.delenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", raising=False)
    adapter = APIServerAdapter(
        PlatformConfig(enabled=True, extra={"key": "api-key-that-is-long-enough"})
    )
    assert "/v1/platform-turns/matrix" not in [
        path for _, path, _ in adapter._http_route_table()
    ]

    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", "api-key-that-is-long-enough")
    adapter = APIServerAdapter(
        PlatformConfig(enabled=True, extra={"key": "api-key-that-is-long-enough"})
    )
    assert "/v1/platform-turns/matrix" not in [
        path for _, path, _ in adapter._http_route_table()
    ]

    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    adapter = APIServerAdapter(
        PlatformConfig(enabled=True, extra={"key": "api-key-that-is-long-enough"})
    )
    assert [path for _, path, _ in adapter._http_route_table()].count(
        "/v1/platform-turns/matrix"
    ) == 1


def test_api_server_active_work_count_includes_trusted_ingress(monkeypatch):
    from gateway.platforms.api_server import APIServerAdapter

    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    adapter = APIServerAdapter(
        PlatformConfig(
            enabled=True,
            extra={"key": "api-key-that-is-long-enough"},
        )
    )
    adapter._trusted_matrix_turn_ingress = SimpleNamespace(inflight_count=2)

    assert adapter.active_agent_work_count() == 2


def test_api_server_active_work_count_does_not_double_count_reserved_trusted_turn(
    monkeypatch,
):
    from gateway.platforms.api_server import APIServerAdapter

    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    adapter = APIServerAdapter(
        PlatformConfig(
            enabled=True,
            extra={"key": "api-key-that-is-long-enough"},
        )
    )
    adapter._pending_agent_requests = 1
    adapter._pending_trusted_matrix_requests = 1
    adapter._trusted_matrix_turn_ingress = SimpleNamespace(inflight_count=1)

    assert adapter.active_agent_work_count() == 1


@pytest.mark.asyncio
async def test_http_route_rejects_drain_before_reading_body(monkeypatch):
    from gateway.platforms.api_server import APIServerAdapter

    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    adapter = APIServerAdapter(
        PlatformConfig(
            enabled=True,
            extra={"key": "api-key-that-is-long-enough"},
        )
    )
    monkeypatch.setattr(adapter, "_gateway_is_draining", lambda: True)

    class Content:
        async def iter_chunked(self, _size):
            raise AssertionError("draining requests must not read the body")
            yield b""  # pragma: no cover

    request = SimpleNamespace(
        content_length=None,
        content=Content(),
        headers={"Authorization": f"Bearer {INGRESS_KEY}"},
    )

    response = await adapter._handle_trusted_matrix_platform_turn(request)
    assert response.status == 503


@pytest.mark.asyncio
async def test_http_route_returns_exact_answer_posted_by_native_turn(monkeypatch):
    aiohttp = pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms.api_server import APIServerAdapter

    assert aiohttp is not None
    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    adapter = APIServerAdapter(
        PlatformConfig(enabled=True, extra={"key": "api-key-that-is-long-enough"})
    )
    runner = _FakeRunner()
    adapter.gateway_runner = runner
    app = web.Application()
    app.router.add_post(
        "/v1/platform-turns/matrix", adapter._handle_trusted_matrix_platform_turn
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            "/v1/platform-turns/matrix",
            data=_request(),
            headers={
                "Authorization": f"Bearer {INGRESS_KEY}",
                "Content-Type": "application/json",
            },
        )
        body = await response.json()
        assert response.status == 200
        assert body["answer"] == "Exact Matrix answer"
        assert body["matrix"] == {
            "input_event_id": "$input",
            "reply_event_id": "$reply",
        }
        assert body["audio"] is None
        assert response.headers["Cache-Control"] == "no-store"
        assert len(runner.calls) == 1
    finally:
        await client.close()


def _parse_sse_events(payload: str):
    events = []
    comments = []
    for block in payload.split("\n\n"):
        if not block:
            continue
        if block.startswith(":"):
            comments.append(block)
            continue
        lines = block.splitlines()
        name = next(line[7:] for line in lines if line.startswith("event: "))
        data = next(line[6:] for line in lines if line.startswith("data: "))
        events.append((name, json.loads(data)))
    return events, comments


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("request_overrides", "extra_headers"),
    [
        ({"stream": True}, {}),
        ({}, {"Accept": "text/event-stream"}),
    ],
)
async def test_http_route_streams_sse_for_json_flag_or_accept_header(
    monkeypatch,
    request_overrides,
    extra_headers,
):
    aiohttp = pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms.api_server import APIServerAdapter

    assert aiohttp is not None
    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    adapter = APIServerAdapter(
        PlatformConfig(enabled=True, extra={"key": "api-key-that-is-long-enough"})
    )
    runner = _FakeRunner()
    adapter.gateway_runner = runner
    app = web.Application()
    app.router.add_post(
        "/v1/platform-turns/matrix",
        adapter._handle_trusted_matrix_platform_turn,
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            "/v1/platform-turns/matrix",
            data=_request(
                request_id=f"sse-{len(extra_headers)}",
                **request_overrides,
            ),
            headers={
                "Authorization": f"Bearer {INGRESS_KEY}",
                "Content-Type": "application/json",
                **extra_headers,
            },
        )
        events, comments = _parse_sse_events(await response.text())

        assert response.status == 200
        assert response.headers["Content-Type"].startswith("text/event-stream")
        assert response.headers["Cache-Control"] == "no-store"
        assert response.headers["X-Accel-Buffering"] == "no"
        assert comments == []
        assert [name for name, _data in events] == [
            "delta",
            "delta",
            "delta",
            "done",
        ]
        assert [data for name, data in events if name == "delta"] == [
            {"text": delta} for delta in runner.deltas
        ]
        assert events[-1][1]["answer"] == "Exact Matrix answer"
        assert events[-1][1]["matrix"] == {
            "input_event_id": "$input",
            "reply_event_id": "$reply",
        }
        assert len(runner.calls) == 1
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provisional", ["I will inspect that first.", 'Checking “time” 🕓\nOne moment…']
)
async def test_http_stream_replaces_provisional_text_before_authoritative_done(
    monkeypatch, provisional
):
    aiohttp = pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms.api_server import APIServerAdapter

    assert aiohttp is not None
    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    adapter = APIServerAdapter(
        PlatformConfig(enabled=True, extra={"key": "api-key-that-is-long-enough"})
    )
    runner = _FakeRunner()
    runner.deltas = [provisional]
    adapter.gateway_runner = runner
    app = web.Application()
    app.router.add_post(
        "/v1/platform-turns/matrix",
        adapter._handle_trusted_matrix_platform_turn,
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            "/v1/platform-turns/matrix",
            data=_request(request_id="sse-replace", stream=True),
            headers={"Authorization": f"Bearer {INGRESS_KEY}"},
        )
        events, _comments = _parse_sse_events(await response.text())

        assert [name for name, _data in events] == ["delta", "replace", "done"]
        # These text objects are decoded by the iPhone's TextPayload, not strings.
        assert events[0][1] == {"text": provisional}
        assert events[1][1] == {"text": "Exact Matrix answer"}
        assert events[2][1]["answer"] == "Exact Matrix answer"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_http_stream_done_preserves_optional_tts_audio_payload(monkeypatch):
    aiohttp = pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms.api_server import APIServerAdapter

    assert aiohttp is not None
    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    runner = _FakeRunner()

    async def synthesize(text):
        assert text == "Exact Matrix answer"
        return _wav_bytes()

    adapter = APIServerAdapter(
        PlatformConfig(enabled=True, extra={"key": "api-key-that-is-long-enough"})
    )
    adapter._trusted_matrix_turn_ingress = TrustedMatrixTurnIngress(
        lambda: runner,
        ingress_key=INGRESS_KEY,
        api_server_key="api-key-that-is-long-enough",
        wav_synthesizer=synthesize,
    )
    adapter.gateway_runner = runner
    app = web.Application()
    app.router.add_post(
        "/v1/platform-turns/matrix",
        adapter._handle_trusted_matrix_platform_turn,
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            "/v1/platform-turns/matrix",
            data=_request(request_id="sse-tts", stream=True, tts=True),
            headers={"Authorization": f"Bearer {INGRESS_KEY}"},
        )
        events, _comments = _parse_sse_events(await response.text())

        assert events[-1][0] == "done"
        done = events[-1][1]
        assert done["audio"]["format"] == "wav"
        assert done["audio"]["mime_type"] == "audio/wav"
        assert base64.b64decode(done["audio"]["data_base64"], validate=True) == (
            _wav_bytes()
        )
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_http_stream_heartbeat_stops_before_done(monkeypatch):
    aiohttp = pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms.api_server import APIServerAdapter

    assert aiohttp is not None
    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    runner = _FakeRunner()
    runner.delay = 0.04
    adapter = APIServerAdapter(
        PlatformConfig(enabled=True, extra={"key": "api-key-that-is-long-enough"})
    )
    adapter._trusted_matrix_turn_ingress = TrustedMatrixTurnIngress(
        lambda: runner,
        ingress_key=INGRESS_KEY,
        api_server_key="api-key-that-is-long-enough",
        stream_keepalive_seconds=0.01,
    )
    adapter.gateway_runner = runner
    app = web.Application()
    app.router.add_post(
        "/v1/platform-turns/matrix",
        adapter._handle_trusted_matrix_platform_turn,
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            "/v1/platform-turns/matrix",
            data=_request(stream=True),
            headers={"Authorization": f"Bearer {INGRESS_KEY}"},
        )
        payload = await response.text()
        events, comments = _parse_sse_events(payload)

        assert comments
        assert all(comment == ": keepalive" for comment in comments)
        assert events[-1][0] == "done"
        assert payload.rfind(": keepalive") < payload.rfind("event: done")
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_http_stream_disconnect_detaches_without_cancelling_turn(monkeypatch):
    aiohttp = pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms.api_server import APIServerAdapter

    assert aiohttp is not None
    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    first_delta = asyncio.Event()
    release = asyncio.Event()
    completed = asyncio.Event()

    class DisconnectRunner(_FakeRunner):
        async def process_external_turn(self, request_id, event, on_delta=None):
            self.calls.append((request_id, event))
            self.delta_callbacks.append(on_delta)
            assert on_delta is not None
            on_delta("Exact ")
            first_delta.set()
            await release.wait()
            on_delta("Matrix answer")
            completed.set()
            return ExternalTurnResult("Exact Matrix answer", "$input", "$reply")

    runner = DisconnectRunner()
    adapter = APIServerAdapter(
        PlatformConfig(enabled=True, extra={"key": "api-key-that-is-long-enough"})
    )
    adapter.gateway_runner = runner
    app = web.Application()
    app.router.add_post(
        "/v1/platform-turns/matrix",
        adapter._handle_trusted_matrix_platform_turn,
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            "/v1/platform-turns/matrix",
            data=_request(stream=True),
            headers={"Authorization": f"Bearer {INGRESS_KEY}"},
        )
        assert (await response.content.readline()) == b"event: delta\n"
        assert json.loads((await response.content.readline())[6:]) == {"text": "Exact "}
        await asyncio.wait_for(first_delta.wait(), timeout=1)
        response.close()
        release.set()

        await asyncio.wait_for(completed.wait(), timeout=1)
        for _ in range(100):
            if adapter._trusted_matrix_turn_ingress.inflight_count == 0:
                break
            await asyncio.sleep(0.01)
        assert adapter._trusted_matrix_turn_ingress.inflight_count == 0
        assert len(runner.calls) == 1
    finally:
        release.set()
        await client.close()


@pytest.mark.asyncio
async def test_http_stream_runtime_error_has_one_redacted_terminal_event(monkeypatch):
    aiohttp = pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms.api_server import APIServerAdapter

    assert aiohttp is not None
    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)

    class FailingRunner(_FakeRunner):
        async def process_external_turn(self, request_id, event, on_delta=None):
            self.calls.append((request_id, event))
            raise ExternalTurnUnavailable("matrix_disconnected")

    adapter = APIServerAdapter(
        PlatformConfig(enabled=True, extra={"key": "api-key-that-is-long-enough"})
    )
    adapter.gateway_runner = FailingRunner()
    app = web.Application()
    app.router.add_post(
        "/v1/platform-turns/matrix",
        adapter._handle_trusted_matrix_platform_turn,
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            "/v1/platform-turns/matrix",
            data=_request(stream=True),
            headers={"Authorization": f"Bearer {INGRESS_KEY}"},
        )
        events, _comments = _parse_sse_events(await response.text())

        assert events == [
            (
                "error",
                {
                    "code": "matrix_disconnected",
                    "message": "Matrix turn is unavailable.",
                },
            )
        ]
        assert INGRESS_KEY not in json.dumps(events)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_http_route_streams_body_with_its_narrow_limit(monkeypatch):
    from gateway.platforms.api_server import APIServerAdapter

    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    adapter = APIServerAdapter(
        PlatformConfig(enabled=True, extra={"key": "api-key-that-is-long-enough"})
    )

    class Content:
        async def iter_chunked(self, _size):
            yield b"{" + (b" " * 70_000) + b"}"

    request = SimpleNamespace(
        content_length=None,
        content=Content(),
        headers={"Authorization": f"Bearer {INGRESS_KEY}"},
        read=AsyncMock(side_effect=AssertionError("must not buffer request.read()")),
    )
    response = await adapter._handle_trusted_matrix_platform_turn(request)
    assert response.status == 413
    request.read.assert_not_awaited()


@pytest.mark.asyncio
async def test_http_route_rejects_wrong_key_before_reading_body(monkeypatch):
    from gateway.platforms.api_server import APIServerAdapter

    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    adapter = APIServerAdapter(
        PlatformConfig(enabled=True, extra={"key": "api-key-that-is-long-enough"})
    )

    class UnreadableContent:
        async def iter_chunked(self, _size):
            raise AssertionError("unauthenticated request body must not be read")
            yield b""  # pragma: no cover - keeps this an async generator

    request = SimpleNamespace(
        content_length=None,
        content=UnreadableContent(),
        headers={
            "Authorization": "Bearer wrong-key",
            "Accept": "text/event-stream",
        },
    )

    response = await adapter._handle_trusted_matrix_platform_turn(request)

    assert response.status == 401
    assert response.content_type == "application/json"
    assert json.loads(response.body)["error"]["code"] == "unauthorized"


@pytest.mark.asyncio
async def test_http_route_rejects_lone_surrogate_before_fingerprinting(
    monkeypatch, tmp_path
):
    aiohttp = pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms.api_server import APIServerAdapter

    assert aiohttp is not None
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("MATRIX_PLATFORM_TURN_INGRESS_KEY", INGRESS_KEY)
    adapter = APIServerAdapter(
        PlatformConfig(enabled=True, extra={"key": "api-key-that-is-long-enough"})
    )
    runner = _FakeRunner()
    adapter.gateway_runner = runner
    app = web.Application()
    app.router.add_post(
        "/v1/platform-turns/matrix", adapter._handle_trusted_matrix_platform_turn
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            "/v1/platform-turns/matrix",
            data=(
                b'{"request_id":"unicode-probe","text":"\\ud800",'
                b'"tts":false}'
            ),
            headers={
                "Authorization": f"Bearer {INGRESS_KEY}",
                "Content-Type": "application/json",
            },
        )
        body = await response.json()
        assert response.status == 422
        assert body["error"]["code"] == "invalid_text"
        assert runner.calls == []
        assert not (
            tmp_path
            / "platforms"
            / "matrix"
            / "trusted_matrix_turn_ingress.db"
        ).exists()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_native_delivery_resolves_exact_final_text_and_reply_event_id():
    class _Adapter(BasePlatformAdapter):
        async def connect(self, *, is_reconnect=False):
            return True

        async def disconnect(self):
            return None

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            return SendResult(success=True, message_id="$reply")

        async def get_chat_info(self, chat_id):
            return {}

    adapter = _Adapter(PlatformConfig(enabled=True), Platform.MATRIX)
    adapter._message_handler = AsyncMock(return_value="Exact Matrix answer")
    adapter._send_with_retry = AsyncMock(
        return_value=SendResult(success=True, message_id="$reply")
    )
    completion = asyncio.get_running_loop().create_future()

    class _Runner:
        def _resolve_external_turn(self, event, **kwargs):
            if not completion.done():
                completion.set_result(kwargs)

    adapter.gateway_runner = _Runner()
    event = MessageEvent(
        text="Andrew (spoken): hello",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
            message_id="$input",
        ),
        message_id="$input",
        internal=True,
        metadata={"external_turn_request_id": "shortcut-001"},
    )
    await adapter.handle_message(event)
    resolved = await asyncio.wait_for(completion, timeout=1)
    assert resolved == {"answer": "Exact Matrix answer", "reply_event_id": "$reply"}
    adapter._send_with_retry.assert_awaited_once()
    assert adapter._send_with_retry.await_args.kwargs["content"] == "Exact Matrix answer"


@pytest.mark.asyncio
async def test_tts_timeout_cancels_remaining_native_media_delivery(tmp_path):
    from gateway.run import GatewayRunner

    media_path = tmp_path / "late-document.pdf"
    media_path.write_bytes(b"late media")
    media_entered = asyncio.Event()
    release_media = asyncio.Event()
    late_media_deliveries = []

    runner = object.__new__(GatewayRunner)
    runner._external_turn_futures = {}
    runner.config = SimpleNamespace(matrix_platform_turn_allowed_room_id=VOICE_MEMOS_ROOM_ID)

    class _Adapter(BasePlatformAdapter):
        async def connect(self, *, is_reconnect=False):
            return True

        async def disconnect(self):
            return None

        async def trusted_turn_readiness(self, room_id):
            assert room_id == VOICE_MEMOS_ROOM_ID
            return True, None

        async def build_trusted_turn_source(
            self, *, room_id, sender, event_id, user_name
        ):
            return self.build_source(
                chat_id=room_id,
                chat_type="group",
                user_id=sender,
                user_name=user_name,
                message_id=event_id,
            )

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            self.text_sends.append(content)
            message_id = "$input" if content.startswith("Andrew (spoken):") else "$reply"
            return SendResult(success=True, message_id=message_id)

        async def send_document(
            self,
            chat_id,
            file_path,
            caption=None,
            file_name=None,
            reply_to=None,
            metadata=None,
            **kwargs,
        ):
            media_entered.set()
            await release_media.wait()
            if self._delivery_cancelled(metadata):
                return SendResult(success=False, error="turn_cancelled")
            late_media_deliveries.append(file_path)
            return SendResult(success=True, message_id="$late-media")

        async def get_chat_info(self, chat_id):
            return {}

        @staticmethod
        def filter_media_delivery_paths(media_files, **kwargs):
            return media_files

    adapter = _Adapter(
        PlatformConfig(enabled=True, typing_indicator=False),
        Platform.MATRIX,
    )
    adapter.text_sends = []
    adapter.gateway_runner = runner
    adapter._message_handler = AsyncMock(
        return_value=f"Exact answer\nMEDIA:{media_path}"
    )
    runner.adapters = {Platform.MATRIX: adapter}
    runner._adapter_for_source = lambda _source: adapter
    runner._peek_session_state = lambda _session_key: None

    async def blocked_tts(_text: str) -> bytes:
        await asyncio.wait_for(media_entered.wait(), timeout=1)
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    ingress = TrustedMatrixTurnIngress(
        lambda: runner,
        ingress_key=INGRESS_KEY,
        api_server_key="different-api-server-key",
        allowed_room_id=VOICE_MEMOS_ROOM_ID,
        timeout_seconds=0.02,
        wav_synthesizer=blocked_tts,
    )

    status, body = await ingress.handle(
        f"Bearer {INGRESS_KEY}",
        _request(
            request_id="tts-timeout-native-media",
            room_id=VOICE_MEMOS_ROOM_ID,
            tts=True,
        ),
    )
    assert (status, body["error"]["code"]) == (504, "turn_timeout")
    assert adapter._active_session_events
    active_event = next(iter(adapter._active_session_events.values()))
    assert active_event.metadata["external_turn_cancelled"] is True

    release_media.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert late_media_deliveries == []
    assert adapter.text_sends == ["Andrew (spoken): What changed today?", "Exact answer"]
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_timed_out_native_turn_suppresses_late_matrix_delivery():
    class _Adapter(BasePlatformAdapter):
        async def connect(self, *, is_reconnect=False):
            return True

        async def disconnect(self):
            return None

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            return SendResult(success=True, message_id="$unexpected")

        async def get_chat_info(self, chat_id):
            return {}

    adapter = _Adapter(PlatformConfig(enabled=True), Platform.MATRIX)
    adapter._message_handler = AsyncMock(return_value="Late Matrix answer")
    adapter._send_with_retry = AsyncMock(
        return_value=SendResult(success=True, message_id="$unexpected")
    )
    completion = asyncio.get_running_loop().create_future()

    class _Runner:
        def _resolve_external_turn(self, event, **kwargs):
            if not completion.done():
                completion.set_result(kwargs)

    adapter.gateway_runner = _Runner()  # type: ignore[assignment]
    event = MessageEvent(
        text="Andrew (spoken): hello",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
            message_id="$input",
        ),
        message_id="$input",
        internal=True,
        metadata={
            "external_turn_request_id": "shortcut-timeout",
            "external_turn_cancelled": True,
        },
    )

    await adapter.handle_message(event)
    resolved = await asyncio.wait_for(completion, timeout=1)

    assert resolved == {"error_code": "turn_cancelled"}
    adapter._send_with_retry.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_turn_rechecks_cancellation_after_post_handler_await():
    post_handler_wait = asyncio.Event()
    release_post_handler = asyncio.Event()

    class _Adapter(BasePlatformAdapter):
        async def connect(self, *, is_reconnect=False):
            return True

        async def disconnect(self):
            return None

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            return SendResult(success=True, message_id="$unexpected")

        async def get_chat_info(self, chat_id):
            return {}

        @staticmethod
        def extract_local_files(content):
            return ["/tmp/trusted-turn-cancellation-probe"], content

        @staticmethod
        def filter_local_delivery_paths(file_paths, **kwargs):
            return file_paths

        async def _extract_response_content(self, *args, **kwargs):
            extracted = await super()._extract_response_content(*args, **kwargs)
            post_handler_wait.set()
            await release_post_handler.wait()
            return extracted

    adapter = _Adapter(
        PlatformConfig(enabled=True, typing_indicator=False), Platform.MATRIX
    )
    adapter._message_handler = AsyncMock(return_value="Late Matrix answer")
    adapter._send_with_retry = AsyncMock(
        return_value=SendResult(success=True, message_id="$unexpected")
    )
    completion = asyncio.get_running_loop().create_future()

    class _Runner:
        def _resolve_external_turn(self, event, **kwargs):
            if not completion.done():
                completion.set_result(kwargs)

    adapter.gateway_runner = _Runner()  # type: ignore[assignment]
    event = MessageEvent(
        text="Andrew (spoken): hello",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
            message_id="$input",
        ),
        message_id="$input",
        internal=True,
        metadata={"external_turn_request_id": "shortcut-post-handler-timeout"},
    )

    await adapter.handle_message(event)
    await asyncio.wait_for(post_handler_wait.wait(), timeout=1)
    event.metadata["external_turn_cancelled"] = True
    release_post_handler.set()
    resolved = await asyncio.wait_for(completion, timeout=1)

    assert resolved == {"error_code": "turn_cancelled"}
    adapter._send_with_retry.assert_not_awaited()
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_native_delivery_propagates_cancel_check_and_resolves_mid_send_cancel():
    class _Adapter(BasePlatformAdapter):
        async def connect(self, *, is_reconnect=False):
            return True

        async def disconnect(self):
            return None

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            return SendResult(success=True, message_id="$unused")

        async def get_chat_info(self, chat_id):
            return {}

    adapter = _Adapter(
        PlatformConfig(enabled=True, typing_indicator=False), Platform.MATRIX
    )
    adapter._message_handler = AsyncMock(return_value="Matrix answer")
    completion = asyncio.get_running_loop().create_future()

    class _Runner:
        def _resolve_external_turn(self, event, **kwargs):
            if not completion.done():
                completion.set_result(kwargs)

    adapter.gateway_runner = _Runner()  # type: ignore[assignment]
    event = MessageEvent(
        text="Andrew (spoken): hello",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
            message_id="$input",
        ),
        message_id="$input",
        internal=True,
        metadata={"external_turn_request_id": "shortcut-mid-send-cancel"},
    )

    async def cancel_during_send(*, metadata, **_kwargs):
        cancel_check = metadata.get("_external_turn_cancel_check")
        assert callable(cancel_check)
        event.metadata["external_turn_cancelled"] = True
        assert cancel_check() is True
        return SendResult(success=False, error="turn_cancelled")

    adapter._send_with_retry = AsyncMock(side_effect=cancel_during_send)

    await adapter.handle_message(event)
    resolved = await asyncio.wait_for(completion, timeout=1)

    assert resolved == {"error_code": "turn_cancelled"}
    adapter._send_with_retry.assert_awaited_once()
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_matrix_text_delivery_rechecks_cancellation_between_chunks(monkeypatch):
    from plugins.platforms.matrix.adapter import MatrixAdapter

    cancelled = False
    client = SimpleNamespace()

    async def send_message_event(*_args):
        nonlocal cancelled
        cancelled = True
        return "$first"

    client.send_message_event = AsyncMock(side_effect=send_message_event)
    client.crypto = None
    adapter = object.__new__(MatrixAdapter)
    adapter._client = client
    adapter._encryption = False
    monkeypatch.setattr(adapter, "format_message", lambda content: content)
    monkeypatch.setattr(
        adapter,
        "truncate_message",
        lambda _content, _limit, _len_fn=None: ["first", "second"],
    )
    adapter.max_message_length = 16_000
    monkeypatch.setattr(
        adapter,
        "_build_text_message_content",
        lambda chunk, _msgtype="m.text": {"msgtype": "m.text", "body": chunk},
    )
    monkeypatch.setattr(
        adapter,
        "_apply_relation_metadata",
        lambda *_args, **_kwargs: None,
    )

    result = await adapter.send(
        HELIX_SUPPORT_ROOM_ID,
        "two chunks",
        metadata={"_external_turn_cancel_check": lambda: cancelled},
    )

    assert result.success is False
    assert result.error == "turn_cancelled"
    assert client.send_message_event.await_count == 1


@pytest.mark.asyncio
async def test_matrix_image_batch_rechecks_cancellation_between_images(monkeypatch):
    from plugins.platforms.matrix.adapter import MatrixAdapter

    cancelled = False
    sent = []
    adapter = object.__new__(MatrixAdapter)

    async def send_image(*, image_url, **_kwargs):
        nonlocal cancelled
        sent.append(image_url)
        cancelled = True
        return SendResult(success=True, message_id=f"${len(sent)}")

    monkeypatch.setattr(adapter, "send_image", send_image)

    await adapter.send_multiple_images(
        HELIX_SUPPORT_ROOM_ID,
        [("https://example.com/one.png", ""), ("https://example.com/two.png", "")],
        metadata={"_external_turn_cancel_check": lambda: cancelled},
    )

    assert sent == ["https://example.com/one.png"]


@pytest.mark.asyncio
async def test_matrix_media_upload_rechecks_cancellation_before_room_event(monkeypatch):
    from plugins.platforms.matrix.adapter import MatrixAdapter

    cancelled = False
    client = SimpleNamespace()

    async def upload_media(*_args, **_kwargs):
        nonlocal cancelled
        cancelled = True
        return "mxc://example.com/uploaded"

    client.upload_media = AsyncMock(side_effect=upload_media)
    client.send_message_event = AsyncMock(return_value="$unexpected")
    client.crypto = None
    adapter = object.__new__(MatrixAdapter)
    adapter._client = client
    adapter._encryption = False
    adapter._max_media_bytes = 1024
    monkeypatch.setattr(
        adapter,
        "_apply_relation_metadata",
        lambda *_args, **_kwargs: None,
    )

    result = await adapter._upload_and_send(
        HELIX_SUPPORT_ROOM_ID,
        b"image bytes",
        "image.png",
        "image/png",
        "m.image",
        metadata={"_external_turn_cancel_check": lambda: cancelled},
    )

    assert result.success is False
    assert result.error == "turn_cancelled"
    client.send_message_event.assert_not_awaited()


@pytest.mark.asyncio
async def test_matrix_media_send_redacts_event_if_cancelled_during_send(monkeypatch):
    from plugins.platforms.matrix.adapter import MatrixAdapter

    cancelled = False
    client = SimpleNamespace()
    client.upload_media = AsyncMock(return_value="mxc://example.com/uploaded")

    async def send_message_event(*_args):
        nonlocal cancelled
        cancelled = True
        return "$late-media"

    client.send_message_event = AsyncMock(side_effect=send_message_event)
    client.crypto = None
    client.redact = AsyncMock(return_value=None)
    adapter = object.__new__(MatrixAdapter)
    adapter._client = client
    adapter._encryption = False
    adapter._max_media_bytes = 1024
    monkeypatch.setattr(
        adapter,
        "_apply_relation_metadata",
        lambda *_args, **_kwargs: None,
    )

    result = await adapter._upload_and_send(
        HELIX_SUPPORT_ROOM_ID,
        b"image bytes",
        "image.png",
        "image/png",
        "m.image",
        metadata={"_external_turn_cancel_check": lambda: cancelled},
    )

    assert result.success is False
    assert result.error == "turn_cancelled"
    client.redact.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handler_result", "expected_code"),
    [
        (None, "agent_no_response"),
        (RuntimeError("agent exploded"), "agent_failed"),
    ],
)
async def test_native_agent_failure_resolves_completion_without_waiting_for_http_timeout(
    handler_result, expected_code
):
    class _Adapter(BasePlatformAdapter):
        async def connect(self, *, is_reconnect=False):
            return True

        async def disconnect(self):
            return None

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            return SendResult(success=True, message_id="$error-notice")

        async def get_chat_info(self, chat_id):
            return {}

    adapter = _Adapter(PlatformConfig(enabled=True), Platform.MATRIX)

    async def handler(_event):
        if isinstance(handler_result, BaseException):
            raise handler_result
        return handler_result

    adapter._message_handler = handler
    completion = asyncio.get_running_loop().create_future()

    class _Runner:
        def _resolve_external_turn(self, event, **kwargs):
            if not completion.done():
                completion.set_result(kwargs)

    adapter.gateway_runner = _Runner()
    event = MessageEvent(
        text="Andrew (spoken): hello",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.MATRIX,
            chat_id=HELIX_SUPPORT_ROOM_ID,
            chat_type="group",
            message_id="$input",
        ),
        message_id="$input",
        internal=True,
        metadata={"external_turn_request_id": "shortcut-failure"},
    )
    await adapter.handle_message(event)
    resolved = await asyncio.wait_for(completion, timeout=1)
    assert resolved == {"error_code": expected_code}
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_cancel_session_processing_does_not_release_replacement_owner_guard():
    class _Adapter(BasePlatformAdapter):
        async def connect(self, *, is_reconnect=False):
            return True

        async def disconnect(self):
            return None

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            return SendResult(success=True, message_id="$unused")

        async def get_chat_info(self, chat_id):
            return {}

    adapter = _Adapter(PlatformConfig(enabled=True), Platform.MATRIX)
    session_key = "agent:main:matrix:group:room"
    guard = asyncio.Event()
    replacement_task = asyncio.create_task(asyncio.Event().wait())

    async def old_owner():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            adapter._session_tasks[session_key] = replacement_task
            raise

    old_task = asyncio.create_task(old_owner())
    await asyncio.sleep(0)
    adapter._active_sessions[session_key] = guard
    adapter._session_tasks[session_key] = old_task

    try:
        await adapter.cancel_session_processing(session_key, discard_pending=False)
        assert adapter._session_tasks[session_key] is replacement_task
        assert adapter._active_sessions[session_key] is guard
    finally:
        replacement_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await replacement_task
