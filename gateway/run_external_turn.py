"""Trusted HTTP turns through the live Matrix adapter and native session lifecycle."""

from __future__ import annotations
import asyncio
from typing import Any, Optional, Callable, Awaitable, cast
from gateway.config import Platform
from gateway.platforms.base import MessageEvent
from gateway.session import SessionSource, build_session_key
from gateway.trusted_matrix_turn import (
    ANDREW_MATRIX_USER_ID,
    HELIX_SUPPORT_ROOM_ID,
    ExternalTurnResult,
    ExternalTurnUnavailable,
    ensure_external_turn_available,
)


class GatewayExternalTurnMixin:
    async def process_external_turn(
        self,
        request_id: str,
        event: MessageEvent,
        on_delta: Optional[Callable[[str], None]] = None,
    ) -> ExternalTurnResult:
        """Post and await one trusted turn through the live Matrix adapter."""
        ensure_external_turn_available(self)
        source = event.source
        allowed_room_id = getattr(
            getattr(self, "config", None),
            "matrix_platform_turn_allowed_room_id",
            HELIX_SUPPORT_ROOM_ID,
        )
        if (
            allowed_room_id is None
            or source is None
            or source.platform != Platform.MATRIX
            or source.chat_id != allowed_room_id
        ):
            raise ExternalTurnUnavailable(
                "room_not_allowed",
                "Target room is not allowed.",
            )

        adapter = self.adapters.get(Platform.MATRIX)
        if adapter is None:
            raise ExternalTurnUnavailable("matrix_disconnected")
        readiness = getattr(adapter, "trusted_turn_readiness", None)
        if not callable(readiness):
            raise ExternalTurnUnavailable("matrix_unavailable")
        ready, reason = await cast(
            Callable[[str], Awaitable[tuple[bool, Optional[str]]]],
            readiness,
        )(source.chat_id)
        if not ready:
            raise ExternalTurnUnavailable(reason or "matrix_unavailable")
        if not callable(getattr(adapter, "_message_handler", None)):
            raise ExternalTurnUnavailable("gateway_unavailable")
        build_source = getattr(adapter, "build_source", None)
        if not callable(build_source):
            raise ExternalTurnUnavailable("matrix_unavailable")
        raw_preflight_builder = getattr(
            adapter,
            "build_trusted_turn_preflight_source",
            None,
        )
        if callable(raw_preflight_builder):
            preflight_builder = cast(
                Callable[..., Awaitable[SessionSource]],
                raw_preflight_builder,
            )
            preflight_source = await preflight_builder(
                room_id=source.chat_id,
                sender=ANDREW_MATRIX_USER_ID,
                user_name="Andrew",
            )
        else:
            preflight_source = cast(
                SessionSource,
                build_source(
                    chat_id=source.chat_id,
                    chat_name=source.chat_name,
                    chat_type="group",
                    user_id=ANDREW_MATRIX_USER_ID,
                    user_name="Andrew",
                    chat_topic=source.chat_topic,
                    guild_id=source.guild_id,
                    parent_chat_id=source.parent_chat_id,
                ),
            )
        if getattr(
            preflight_source, "profile_route_rejected", False
        ) or preflight_source.profile not in (None, "default"):
            raise ExternalTurnUnavailable("profile_scope_mismatch")
        raw_build_trusted_source = getattr(
            adapter,
            "build_trusted_turn_source",
            None,
        )
        if not callable(raw_build_trusted_source):
            raise ExternalTurnUnavailable("matrix_unavailable")
        build_trusted_source = cast(
            Callable[..., Awaitable[SessionSource]],
            raw_build_trusted_source,
        )

        futures = self.__dict__.setdefault("_external_turn_futures", {})
        if request_id in futures:
            raise ExternalTurnUnavailable(
                "request_in_progress",
                "A turn with this request_id is already running.",
            )
        future = asyncio.get_running_loop().create_future()
        futures[request_id] = future

        spoken_text = f"Andrew (spoken): {event.text.strip()}"
        event.text = spoken_text
        event.internal = True
        event.metadata["external_turn_request_id"] = request_id
        event.metadata["external_turn_force_non_streaming"] = True
        try:
            ensure_external_turn_available(self)
            input_result = await adapter.send(source.chat_id, spoken_text)
            if not input_result.success or not input_result.message_id:
                raise ExternalTurnUnavailable(
                    "matrix_input_delivery_failed",
                    "Matrix input delivery failed.",
                )
            source = cast(
                SessionSource,
                await build_trusted_source(
                    room_id=source.chat_id,
                    sender=ANDREW_MATRIX_USER_ID,
                    event_id=input_result.message_id,
                    user_name="Andrew",
                ),
            )
            if getattr(
                source, "profile_route_rejected", False
            ) or source.profile not in (None, "default"):
                raise ExternalTurnUnavailable("profile_scope_mismatch")
            source.profile = "default"
            source.user_id = ANDREW_MATRIX_USER_ID
            source.user_name = "Andrew"
            setattr(source, "_external_turn_force_non_streaming", True)
            if on_delta is not None:
                # SessionSource serializes through an explicit allowlist, so this
                # process-local callback cannot be forged or persisted.
                setattr(source, "_external_turn_stream_delta_callback", on_delta)
            event.source = source
            event.message_id = input_result.message_id
            source.message_id = input_result.message_id
            await adapter.handle_message(event)
            if not getattr(event, "_gateway_accepted", False):
                raise ExternalTurnUnavailable(
                    "gateway_rejected", "Gateway did not accept the Matrix turn."
                )
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            if (event.metadata or {}).get("external_turn_delivery_started") and not (
                event.metadata or {}
            ).get("external_turn_delivery_finished"):
                try:
                    return await asyncio.wait_for(
                        asyncio.shield(future),
                        timeout=5.0,
                    )
                except asyncio.TimeoutError:
                    pass
            await self._cancel_external_turn_work(event)
            raise
        finally:
            if not future.done():
                future.cancel()
            if futures.get(request_id) is future:
                futures.pop(request_id, None)

    async def _cancel_external_turn_work(self, event: MessageEvent) -> None:
        """Remove or interrupt only the timed-out trusted turn."""
        adapter = cast(Any, self._adapter_for_source(event.source))
        event.metadata["external_turn_cancelled"] = True
        if adapter is None:
            return
        session_key = build_session_key(
            event.source,
            group_sessions_per_user=adapter.config.extra.get(
                "group_sessions_per_user", True
            ),
            thread_sessions_per_user=adapter.config.extra.get(
                "thread_sessions_per_user", False
            ),
        )

        def matches(candidate: Optional[MessageEvent]) -> bool:
            return bool(
                candidate is event
                or (
                    candidate is not None
                    and (candidate.metadata or {}).get("external_turn_request_id")
                    == (event.metadata or {}).get("external_turn_request_id")
                )
            )

        pending = adapter._pending_messages.get(session_key)
        if matches(pending):
            adapter._pending_messages.pop(session_key, None)
            promoted = self._promote_queued_event(session_key, adapter, None)
            if promoted is not None:
                adapter._pending_messages[session_key] = promoted

        state = self._peek_session_state(session_key)
        if state is not None:
            state.conversation.queued_events[:] = [
                candidate
                for candidate in state.conversation.queued_events
                if not matches(candidate)
            ]

        active = adapter.__dict__.get("_active_session_events", {}).get(session_key)
        if matches(active):
            active_state = self._peek_session_state(session_key)
            running_agent = (
                active_state.turn.agent if active_state is not None else None
            )
            interrupt = getattr(running_agent, "interrupt", None)
            if callable(interrupt):
                interrupt("Trusted Matrix platform turn timed out.")
            active_guard = adapter.__dict__.get("_active_sessions", {}).get(session_key)
            if active_guard is not None:
                active_guard.set()

    def _resolve_external_turn(
        self,
        event: MessageEvent,
        *,
        answer: Optional[str] = None,
        reply_event_id: Optional[str] = None,
        error_code: Optional[str] = None,
    ) -> None:
        """Resolve the completion future after native Matrix delivery."""
        request_id = (event.metadata or {}).get("external_turn_request_id")
        if not request_id:
            return
        futures = self.__dict__.get("_external_turn_futures") or {}
        future = futures.get(request_id)
        if future is None or future.done():
            return
        if error_code or answer is None or not reply_event_id:
            future.set_exception(
                ExternalTurnUnavailable(
                    error_code or "matrix_reply_delivery_failed",
                    "Matrix reply delivery failed.",
                )
            )
            return
        future.set_result(
            ExternalTurnResult(
                answer=answer,
                input_event_id=event.message_id or "",
                reply_event_id=reply_event_id,
            )
        )
