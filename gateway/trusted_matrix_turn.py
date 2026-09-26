"""Trusted sync/SSE ingress for one native Matrix gateway turn.

This module owns only the HTTP-facing policy and idempotency lifecycle.  The
actual turn remains in :class:`gateway.run.GatewayRunner` and the already-live
Matrix adapter, so there is exactly one Matrix client and one crypto-store
owner.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import io
import json
import os
import re
import sqlite3
import threading
import tempfile
import time
import wave
from collections import OrderedDict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from gateway.config import (
    DEFAULT_MATRIX_PLATFORM_TURN_ALLOWED_ROOM_ID,
    resolve_matrix_platform_turn_allowed_room_id,
)
from hermes_constants import get_hermes_home


HELIX_SUPPORT_ROOM_ID = DEFAULT_MATRIX_PLATFORM_TURN_ALLOWED_ROOM_ID
ANDREW_MATRIX_USER_ID = "@andrew:eightstory.com"
MATRIX_PLATFORM_TURN_INGRESS_KEY_ENV = "MATRIX_PLATFORM_TURN_INGRESS_KEY"
LOCAL_KOKORO_PROVIDER = "local-kokoro"
LOCAL_KOKORO_VOICE = "af_jessica"
LOCAL_KOKORO_MODEL = "mlx-community/Kokoro-82M-bf16"
MAX_BODY_BYTES = 64 * 1024
MAX_TEXT_CHARS = 4_000
MAX_REQUEST_ID_CHARS = 128
MAX_WAV_BYTES = 12 * 1024 * 1024
DEFAULT_TIMEOUT_SECONDS = 180.0
DEFAULT_STREAM_KEEPALIVE_SECONDS = 10.0
DEFAULT_RATE_LIMIT_REQUESTS = 10
DEFAULT_RATE_LIMIT_WINDOW_SECONDS = 60.0
IDEMPOTENCY_RETENTION_SECONDS = 90 * 24 * 60 * 60
IDEMPOTENCY_MAX_TOMBSTONES = (
    int(IDEMPOTENCY_RETENTION_SECONDS // DEFAULT_RATE_LIMIT_WINDOW_SECONDS) + 1
) * DEFAULT_RATE_LIMIT_REQUESTS
_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_ALLOWED_FIELDS = frozenset({
    "request_id",
    "room_id",
    "text",
    "tts",
    "audio_format",
    "stream",
})


@dataclass(frozen=True)
class ExternalTurnResult:
    """Exact Matrix delivery result returned by ``GatewayRunner``."""

    answer: str
    input_event_id: str
    reply_event_id: str


class ExternalTurnUnavailable(RuntimeError):
    """Fail-closed native Matrix readiness or delivery failure."""

    def __init__(self, code: str, message: str = "Matrix turn is unavailable.") -> None:
        super().__init__(message)
        self.code = code


def ensure_external_turn_available(runner: Any) -> None:
    """Spoken user requests respect pause and require native local execution."""
    from agent.estop import is_engaged

    if is_engaged():
        raise ExternalTurnUnavailable(
            "gateway_paused", "Gateway is paused. Resume Hermes to continue."
        )
    proxy_url = getattr(runner, "_get_proxy_url", None)
    if callable(proxy_url) and proxy_url():
        raise ExternalTurnUnavailable(
            "proxy_not_supported", "Talk to Helix requires local gateway execution."
        )


class _IdempotencyConflict(RuntimeError):
    pass


class _IdempotencyReplayUnavailable(RuntimeError):
    pass


class _DurableRequestJournal:
    """Small persistent tombstone index for crash-safe request admission.

    Full responses remain in the bounded in-memory replay cache. The journal
    persists only a request fingerprint, so a process restart cannot repeat
    Matrix/tool side effects and large base64 audio never bloats the database.
    """

    def __init__(
        self,
        path: Path,
        *,
        retention_seconds: float = IDEMPOTENCY_RETENTION_SECONDS,
        max_tombstones: int = IDEMPOTENCY_MAX_TOMBSTONES,
    ) -> None:
        self._path = path
        self._retention_seconds = retention_seconds
        self._max_tombstones = max_tombstones
        self._lock = threading.Lock()

    def _connect(self) -> sqlite3.Connection:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self._path, timeout=5.0)
        try:
            from hermes_state_wal import apply_wal_with_fallback

            apply_wal_with_fallback(
                connection, db_label="trusted_matrix_turn_ingress.db"
            )
            connection.execute("PRAGMA busy_timeout=5000")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS request_tombstones (
                    request_id TEXT PRIMARY KEY,
                    fingerprint TEXT NOT NULL,
                    created_at REAL NOT NULL
                )"""
            )
            connection.execute(
                """CREATE INDEX IF NOT EXISTS request_tombstones_created_at_idx
                   ON request_tombstones (created_at)"""
            )
            connection.commit()
            try:
                os.chmod(self._path, 0o600)
            except OSError:
                pass
            return connection
        except Exception:
            connection.close()
            raise

    def _claim_sync(self, request_id: str, fingerprint: str) -> str:
        with self._lock:
            connection = self._connect()
            try:
                now = time.time()
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "DELETE FROM request_tombstones WHERE created_at < ?",
                    (now - self._retention_seconds,),
                )
                row = connection.execute(
                    "SELECT fingerprint FROM request_tombstones WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                if row is not None:
                    connection.commit()
                    return "replay" if row[0] == fingerprint else "conflict"

                count = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM request_tombstones"
                    ).fetchone()[0]
                )
                overflow = count - self._max_tombstones + 1
                if overflow > 0:
                    connection.execute(
                        """DELETE FROM request_tombstones WHERE request_id IN (
                            SELECT request_id FROM request_tombstones
                            ORDER BY created_at ASC LIMIT ?
                        )""",
                        (overflow,),
                    )
                connection.execute(
                    """INSERT INTO request_tombstones
                       (request_id, fingerprint, created_at) VALUES (?, ?, ?)""",
                    (request_id, fingerprint, now),
                )
                connection.commit()
                return "claimed"
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    def _release_sync(self, request_id: str, fingerprint: str) -> None:
        with self._lock:
            connection = self._connect()
            try:
                connection.execute(
                    """DELETE FROM request_tombstones
                       WHERE request_id = ? AND fingerprint = ?""",
                    (request_id, fingerprint),
                )
                connection.commit()
            finally:
                connection.close()

    async def claim(self, request_id: str, fingerprint: str) -> str:
        return await asyncio.to_thread(self._claim_sync, request_id, fingerprint)

    async def release(self, request_id: str, fingerprint: str) -> None:
        await asyncio.to_thread(self._release_sync, request_id, fingerprint)


class _TurnCache:
    """Bounded in-memory replay cache and single-flight registry."""

    def __init__(
        self,
        *,
        max_items: int = 512,
        ttl_seconds: float = 900.0,
        max_response_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        self._store: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
        self._inflight: dict[str, dict[str, Any]] = {}
        self._max_items = max_items
        self._ttl_seconds = ttl_seconds
        self._max_response_bytes = max_response_bytes
        self._response_bytes = 0

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)

    @staticmethod
    def _response_size(response: tuple[int, dict[str, Any]]) -> int:
        return (
            len(
                json.dumps(
                    response[1], separators=(",", ":"), ensure_ascii=False
                ).encode("utf-8")
            )
            + 16
        )

    def _remove(self, request_id: str) -> None:
        item = self._store.pop(request_id, None)
        if item is not None:
            self._response_bytes -= int(item.get("response_size") or 0)

    @staticmethod
    def _deliver_stream_event(
        callback: Callable[[str, str], None],
        event_name: str,
        data: str,
    ) -> None:
        try:
            callback(event_name, data)
        except Exception:
            # A detached/broken HTTP subscriber must never break the native
            # Matrix turn or another subscriber sharing the same flight.
            pass

    def publish_stream_event(
        self,
        request_id: str,
        event_name: str,
        data: str,
    ) -> None:
        """Record and fan out one provisional or replacement stream event."""

        inflight = self._inflight.get(request_id)
        if inflight is None or event_name not in {"delta", "replace"} or not data:
            return
        loop = inflight["loop"]

        def publish() -> None:
            current = self._inflight.get(request_id)
            if current is not inflight:
                return
            assert current is not None
            current["stream_events"].append((event_name, data))
            for callback in tuple(current["listeners"]):
                self._deliver_stream_event(callback, event_name, data)

        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if running_loop is loop:
            publish()
        else:
            try:
                loop.call_soon_threadsafe(publish)
            except RuntimeError:
                pass

    def _purge(self) -> None:
        now = time.monotonic()
        expired = [
            key
            for key, item in self._store.items()
            if now - item["stored_at"] > self._ttl_seconds
        ]
        for key in expired:
            self._remove(key)
        while len(self._store) > self._max_items:
            oldest = next(iter(self._store))
            self._remove(oldest)

        # Audio-bearing JSON responses can be many MiB because WAV bytes are
        # base64 encoded. Keep the replay cache memory-bounded while retaining
        # a lightweight fingerprint tombstone, so an evicted duplicate can
        # never create a second Matrix event.
        for item in self._store.values():
            if self._response_bytes <= self._max_response_bytes:
                break
            old_size = int(item.get("response_size") or 0)
            status, body = item["response"]
            if status == 409 and body.get("error", {}).get("code") == (
                "idempotency_response_evicted"
            ):
                continue
            compact = _error(
                409,
                "idempotency_response_evicted",
                "The completed turn will not be repeated; its response was evicted.",
            )
            new_size = self._response_size(compact)
            item["response"] = compact
            item["stream_events"] = []
            item["response_size"] = new_size
            self._response_bytes += new_size - old_size

    async def get_or_compute(
        self,
        request_id: str,
        fingerprint: str,
        compute: Callable[[], Awaitable[tuple[int, dict[str, Any]]]],
        *,
        on_stream_event: Optional[Callable[[str, str], None]] = None,
    ) -> tuple[int, dict[str, Any]]:
        self._purge()
        cached = self._store.get(request_id)
        if cached is not None:
            if cached["fingerprint"] != fingerprint:
                raise _IdempotencyConflict
            self._store.move_to_end(request_id)
            if on_stream_event is not None:
                for event_name, data in cached.get("stream_events", ()):
                    self._deliver_stream_event(on_stream_event, event_name, data)
            return cached["response"]

        inflight = self._inflight.get(request_id)
        if inflight is not None:
            if inflight["fingerprint"] != fingerprint:
                raise _IdempotencyConflict
            if on_stream_event is not None:
                for event_name, data in tuple(inflight["stream_events"]):
                    self._deliver_stream_event(on_stream_event, event_name, data)
                inflight["listeners"].append(on_stream_event)
            try:
                return await asyncio.shield(inflight["task"])
            finally:
                if on_stream_event is not None:
                    try:
                        inflight["listeners"].remove(on_stream_event)
                    except ValueError:
                        pass

        flight: dict[str, Any] = {
            "fingerprint": fingerprint,
            "task": None,
            "stream_events": [],
            "listeners": [on_stream_event] if on_stream_event is not None else [],
            "loop": asyncio.get_running_loop(),
        }

        async def _run() -> tuple[int, dict[str, Any]]:
            response = await compute()
            # Thread-originated stream events use call_soon_threadsafe(); let all
            # already-enqueued publications land before snapshotting history.
            await asyncio.sleep(0)
            # Unique rate-limited IDs caused no Matrix side effect and must not
            # be allowed to flood the replay index.
            if response[0] != 429:
                stream_events = list(flight["stream_events"])
                response_size = self._response_size(response) + sum(
                    len(event_name.encode("utf-8")) + len(data.encode("utf-8"))
                    for event_name, data in stream_events
                )
                self._store[request_id] = {
                    "fingerprint": fingerprint,
                    "response": response,
                    "stream_events": stream_events,
                    "response_size": response_size,
                    "stored_at": time.monotonic(),
                }
                self._response_bytes += response_size
                self._purge()
            return response

        task = asyncio.create_task(_run())
        flight["task"] = task
        self._inflight[request_id] = flight

        def _clear_inflight(done_task: asyncio.Task) -> None:
            current = self._inflight.get(request_id)
            if current is not flight:
                return
            assert current is not None
            if current["task"] is done_task:
                self._inflight.pop(request_id, None)

        task.add_done_callback(_clear_inflight)
        try:
            return await asyncio.shield(task)
        finally:
            if on_stream_event is not None:
                try:
                    flight["listeners"].remove(on_stream_event)
                except ValueError:
                    pass
            current = self._inflight.get(request_id)
            if current is flight:
                assert current is not None
                if current["task"] is task and task.done():
                    self._inflight.pop(request_id, None)


class _RateLimiter:
    def __init__(self, *, limit: int, window_seconds: float) -> None:
        self._limit = limit
        self._window_seconds = window_seconds
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def admit(self) -> bool:
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self._window_seconds
            while self._timestamps and self._timestamps[0] <= cutoff:
                self._timestamps.popleft()
            if len(self._timestamps) >= self._limit:
                return False
            self._timestamps.append(now)
            return True


def _error(status: int, code: str, message: str) -> tuple[int, dict[str, Any]]:
    return status, {"ok": False, "error": {"code": code, "message": message}}


def _fingerprint(body: dict[str, Any]) -> str:
    canonical = json.dumps(
        body, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _valid_wav(data: bytes) -> bool:
    if not data or len(data) > MAX_WAV_BYTES:
        return False
    try:
        with wave.open(io.BytesIO(data), "rb") as wav_file:
            channels = wav_file.getnchannels()
            sample_width = wav_file.getsampwidth()
            frame_count = wav_file.getnframes()
            frames = wav_file.readframes(frame_count)
            return (
                wav_file.getcomptype() == "NONE"
                and channels > 0
                and sample_width > 0
                and wav_file.getframerate() > 0
                and frame_count > 0
                and len(frames) == frame_count * channels * sample_width
            )
    except (EOFError, wave.Error):
        return False


def _require_local_kokoro_config() -> tuple[dict[str, Any], dict[str, Any]]:
    """Fail closed unless the named command provider is the approved voice/model."""

    try:
        from hermes_cli.config import load_config

        config = load_config()
        tts_config = config.get("tts") if isinstance(config, dict) else None
        providers = (
            tts_config.get("providers") if isinstance(tts_config, dict) else None
        )
        provider = (
            providers.get(LOCAL_KOKORO_PROVIDER)
            if isinstance(providers, dict)
            else None
        )
        command = (
            str(provider.get("command") or "") if isinstance(provider, dict) else ""
        )
        required_placeholders = ("voice", "model", "format", "output_path")
        valid = (
            isinstance(provider, dict)
            and str(provider.get("type") or "").strip().lower() == "command"
            and all(
                f"{{{name}}}" in command or f"{{{{{name}}}}}" in command
                for name in required_placeholders
            )
            and (
                "{input_path}" in command
                or "{{input_path}}" in command
                or "{text_path}" in command
                or "{{text_path}}" in command
            )
            and str(provider.get("voice") or "").strip() == LOCAL_KOKORO_VOICE
            and str(provider.get("model") or "").strip() == LOCAL_KOKORO_MODEL
        )
    except Exception as exc:
        raise ExternalTurnUnavailable(
            "tts_misconfigured", "The approved local Kokoro provider is unavailable."
        ) from exc
    if not valid:
        raise ExternalTurnUnavailable(
            "tts_misconfigured", "The approved local Kokoro provider is unavailable."
        )
    assert isinstance(provider, dict) and isinstance(tts_config, dict)
    return ({str(key): value for key, value in provider.items()}, dict(tts_config))


async def synthesize_local_kokoro_wav(text: str) -> bytes:
    """Synthesize one exact final answer using the configured local Kokoro provider."""

    provider_config, tts_config = _require_local_kokoro_config()
    # The user's normal voice-message provider may default to OGG. This narrow
    # API contract requires WAV, so invoke the same command provider with a
    # per-call format override rather than mutating config or transcoding.
    provider_config["output_format"] = "wav"
    provider_config["format"] = "wav"
    audio_dir = get_hermes_home() / "audio_cache" / "trusted_matrix_turns"
    audio_dir.mkdir(parents=True, exist_ok=True)
    fd, requested_path = tempfile.mkstemp(prefix="turn_", suffix=".wav", dir=audio_dir)
    os.close(fd)
    Path(requested_path).unlink(missing_ok=True)
    produced_path: Optional[Path] = None
    cancel_event = threading.Event()
    worker: Optional[asyncio.Task] = None
    try:
        from tools.tts_command_provider import _generate_command_tts

        worker = asyncio.create_task(
            asyncio.to_thread(
                _generate_command_tts,
                text,
                requested_path,
                LOCAL_KOKORO_PROVIDER,
                provider_config,
                tts_config,
                cancel_event,
            )
        )
        generated_path = await asyncio.shield(worker)
        produced_path = Path(generated_path or requested_path)
        if not produced_path.is_file() or produced_path.stat().st_size > MAX_WAV_BYTES:
            raise ExternalTurnUnavailable(
                "invalid_tts_audio", "Speech synthesis did not produce valid WAV audio."
            )
        data = await asyncio.to_thread(produced_path.read_bytes)
        if not _valid_wav(data):
            raise ExternalTurnUnavailable(
                "invalid_tts_audio", "Speech synthesis did not produce valid WAV audio."
            )
        return data
    except ExternalTurnUnavailable:
        raise
    except asyncio.CancelledError:
        cancel_event.set()
        if worker is not None and not worker.done():
            try:
                await asyncio.wait_for(asyncio.shield(worker), timeout=5.0)
            except (asyncio.TimeoutError, Exception):
                pass
        raise
    except Exception as exc:
        raise ExternalTurnUnavailable("tts_failed", "Speech synthesis failed.") from exc
    finally:
        Path(requested_path).unlink(missing_ok=True)
        if produced_path is not None and produced_path != Path(requested_path):
            produced_path.unlink(missing_ok=True)


class TrustedMatrixTurnIngress:
    """Validate, authenticate, rate-limit, and await a native Matrix turn."""

    def __init__(
        self,
        runner_provider: Callable[[], Any],
        *,
        ingress_key: str,
        api_server_key: str,
        allowed_room_id: Optional[str] = HELIX_SUPPORT_ROOM_ID,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        rate_limit_requests: int = DEFAULT_RATE_LIMIT_REQUESTS,
        rate_limit_window_seconds: float = DEFAULT_RATE_LIMIT_WINDOW_SECONDS,
        stream_keepalive_seconds: float = DEFAULT_STREAM_KEEPALIVE_SECONDS,
        wav_synthesizer: Callable[
            [str], Awaitable[bytes]
        ] = synthesize_local_kokoro_wav,
    ) -> None:
        self._runner_provider = runner_provider
        self._ingress_key = ingress_key.strip()
        self._api_server_key = api_server_key.strip()
        self._allowed_room_id = resolve_matrix_platform_turn_allowed_room_id(
            allowed_room_id
        )
        self._timeout_seconds = max(0.01, min(float(timeout_seconds), 300.0))
        self._stream_keepalive_seconds = max(
            0.01,
            min(float(stream_keepalive_seconds), 60.0),
        )
        self._cache = _TurnCache()
        self._journal = _DurableRequestJournal(
            get_hermes_home()
            / "platforms"
            / "matrix"
            / "trusted_matrix_turn_ingress.db"
        )
        self._rate_limiter = _RateLimiter(
            limit=max(1, min(int(rate_limit_requests), 100)),
            window_seconds=max(1.0, float(rate_limit_window_seconds)),
        )
        self._wav_synthesizer = wav_synthesizer
        try:
            from hermes_cli.auth import has_usable_secret

            usable_secret = has_usable_secret(self._ingress_key, min_length=32)
        except Exception:
            usable_secret = False
        self._enabled = (
            self._allowed_room_id is not None
            and usable_secret
            and not (
                bool(self._api_server_key)
                and hmac.compare_digest(
                    self._ingress_key.encode("utf-8"),
                    self._api_server_key.encode("utf-8"),
                )
            )
        )

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def inflight_count(self) -> int:
        return self._cache.inflight_count

    @property
    def stream_keepalive_seconds(self) -> float:
        return self._stream_keepalive_seconds

    @staticmethod
    def wants_stream(raw_body: bytes, accept: str = "") -> bool:
        """Detect a JSON flag or SSE Accept header transport opt-in."""

        accepted_types = {
            item.split(";", 1)[0].strip().lower()
            for item in str(accept or "").split(",")
        }
        if "text/event-stream" in accepted_types:
            return True
        try:
            body = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return False
        return isinstance(body, dict) and body.get("stream") is True

    def is_authenticated(self, authorization: str) -> bool:
        """Validate the header without inspecting or buffering the request body."""

        if not self._enabled or not authorization.startswith("Bearer "):
            return False
        candidate = authorization[7:]
        try:
            return bool(candidate) and hmac.compare_digest(
                candidate.encode("utf-8"), self._ingress_key.encode("utf-8")
            )
        except UnicodeEncodeError:
            return False

    def _validate(
        self, raw_body: bytes
    ) -> tuple[Optional[dict[str, Any]], Optional[tuple[int, dict[str, Any]]]]:
        if len(raw_body) > MAX_BODY_BYTES:
            return None, _error(413, "body_too_large", "Request body is too large.")
        try:
            body = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None, _error(400, "invalid_json", "Request body must be valid JSON.")
        if not isinstance(body, dict):
            return None, _error(
                422, "invalid_body", "Request body must be a JSON object."
            )
        unknown = set(body) - _ALLOWED_FIELDS
        if unknown:
            return None, _error(
                422, "unknown_field", "Request contains unsupported fields."
            )

        request_id = body.get("request_id")
        if not isinstance(request_id, str) or not _ID_PATTERN.fullmatch(request_id):
            return None, _error(422, "invalid_request_id", "request_id is invalid.")
        room_id = body.get("room_id", self._allowed_room_id)
        if self._allowed_room_id is None or room_id != self._allowed_room_id:
            return None, _error(403, "room_not_allowed", "Target room is not allowed.")
        text = body.get("text")
        if not isinstance(text, str) or not text.strip():
            return None, _error(422, "invalid_text", "text must be a non-empty string.")
        try:
            text.encode("utf-8")
        except UnicodeEncodeError:
            return None, _error(422, "invalid_text", "text must contain valid UTF-8.")
        if len(text) > MAX_TEXT_CHARS:
            return None, _error(
                413, "text_too_long", "text exceeds the maximum length."
            )
        tts = body.get("tts", False)
        if not isinstance(tts, bool):
            return None, _error(422, "invalid_tts", "tts must be a boolean.")
        stream = body.get("stream", False)
        if not isinstance(stream, bool):
            return None, _error(422, "invalid_stream", "stream must be a boolean.")
        audio_format = body.get("audio_format", "wav" if tts else None)
        if audio_format is not None and audio_format != "wav":
            return None, _error(
                422,
                "unsupported_audio_format",
                "audio_format must be wav for this API version.",
            )
        return {
            "request_id": request_id,
            "room_id": self._allowed_room_id,
            "text": text,
            "tts": tts,
            "audio_format": audio_format,
            "stream": stream,
        }, None

    async def handle(
        self,
        authorization: str,
        raw_body: bytes,
        *,
        on_stream_event: Optional[Callable[[str, str], None]] = None,
    ) -> tuple[int, dict[str, Any]]:
        if not self._enabled:
            return _error(404, "not_found", "Not found.")
        if not self.is_authenticated(authorization):
            return _error(401, "unauthorized", "Authentication failed.")

        body, validation_error = self._validate(raw_body)
        if validation_error is not None:
            return validation_error
        assert body is not None
        request_id = body["request_id"]
        # Streaming is a representation choice, not a distinct native turn.
        # Omitted/false/true therefore share idempotency and side-effect identity.
        fingerprint = _fingerprint({
            key: value for key, value in body.items() if key != "stream"
        })

        async def _compute() -> tuple[int, dict[str, Any]]:
            try:
                admission = await self._journal.claim(request_id, fingerprint)
            except Exception:
                return _error(
                    503,
                    "idempotency_unavailable",
                    "Durable request admission is unavailable.",
                )
            if admission == "conflict":
                raise _IdempotencyConflict
            if admission == "replay":
                raise _IdempotencyReplayUnavailable
            if not await self._rate_limiter.admit():
                try:
                    await self._journal.release(request_id, fingerprint)
                except Exception:
                    return _error(
                        503,
                        "idempotency_unavailable",
                        "Durable request admission is unavailable.",
                    )
                return _error(429, "rate_limited", "Too many requests.")
            started = time.monotonic()
            runner = self._runner_provider()
            if runner is None:
                try:
                    await self._journal.release(request_id, fingerprint)
                except Exception:
                    return _error(
                        503,
                        "idempotency_unavailable",
                        "Durable request admission is unavailable.",
                    )
                return _error(503, "gateway_unavailable", "Gateway is unavailable.")

            from gateway.platforms.base import MessageEvent, MessageType
            from gateway.session import SessionSource
            from gateway.config import Platform

            event = MessageEvent(
                text=body["text"],
                message_type=MessageType.TEXT,
                source=SessionSource(
                    platform=Platform.MATRIX,
                    chat_id=body["room_id"],
                    chat_type="group",
                    user_id=ANDREW_MATRIX_USER_ID,
                    user_name="Andrew",
                ),
                raw_message={"trusted_platform_turn": True},
            )

            async def _run_turn() -> tuple[int, dict[str, Any]]:
                streamed_deltas: list[str] = []

                def capture_delta(delta: str) -> None:
                    if not isinstance(delta, str) or not delta:
                        return
                    streamed_deltas.append(delta)
                    self._cache.publish_stream_event(request_id, "delta", delta)

                if on_stream_event is None:
                    result: ExternalTurnResult = await runner.process_external_turn(
                        request_id,
                        event,
                    )
                else:
                    result = await runner.process_external_turn(
                        request_id,
                        event,
                        on_delta=capture_delta,
                    )
                    # Provider callbacks can arrive from the agent worker thread.
                    # Drain queued publications before any authoritative replace.
                    await asyncio.sleep(0)
                    if "".join(streamed_deltas) != result.answer:
                        self._cache.publish_stream_event(
                            request_id,
                            "replace",
                            result.answer,
                        )
                audio = None
                if body["tts"]:
                    wav_data = await self._wav_synthesizer(result.answer)
                    if not _valid_wav(wav_data):
                        raise ExternalTurnUnavailable(
                            "invalid_tts_audio",
                            "Speech synthesis did not produce valid WAV audio.",
                        )
                    audio = {
                        "format": "wav",
                        "mime_type": "audio/wav",
                        "data_base64": base64.b64encode(wav_data).decode("ascii"),
                    }
                elapsed_ms = max(0, int((time.monotonic() - started) * 1000))
                return 200, {
                    "ok": True,
                    "request_id": request_id,
                    "answer": result.answer,
                    "matrix": {
                        "input_event_id": result.input_event_id,
                        "reply_event_id": result.reply_event_id,
                    },
                    "audio": audio,
                    "elapsed_ms": elapsed_ms,
                }

            try:
                return await asyncio.wait_for(
                    _run_turn(),
                    timeout=self._timeout_seconds,
                )
            except asyncio.TimeoutError:
                return _error(504, "turn_timeout", "Matrix turn timed out.")
            except ExternalTurnUnavailable as exc:
                if exc.code == "request_in_progress":
                    status = 409
                elif exc.code in {
                    "matrix_disconnected",
                    "matrix_unavailable",
                    "e2ee_disabled",
                    "olm_unavailable",
                    "room_unencrypted",
                    "gateway_unavailable",
                    "gateway_paused",
                    "proxy_not_supported",
                }:
                    status = 503
                else:
                    status = 502
                return _error(status, exc.code, str(exc))
            except asyncio.CancelledError:
                raise
            except Exception:
                return _error(502, "turn_failed", "Matrix turn failed.")

        try:
            return await self._cache.get_or_compute(
                request_id,
                fingerprint,
                _compute,
                on_stream_event=on_stream_event,
            )
        except _IdempotencyConflict:
            return _error(
                409,
                "idempotency_conflict",
                "request_id was already used with a different payload.",
            )
        except _IdempotencyReplayUnavailable:
            return _error(
                409,
                "idempotency_replay_unavailable",
                "The turn will not be repeated; its prior response is no longer in memory.",
            )
