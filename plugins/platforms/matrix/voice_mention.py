"""Park unmentioned MSC3245 voice messages until the sender's bare @mention claims them.

Element X sends a mention typed while recording as a SEPARATE ``m.text`` event after the
voice event (which carries an empty ``m.mentions``). Under ``require_mention`` the voice is
parked instead of forgotten; a bare mention from the same sender in the same room and
thread within the window claims it. Room-root events form a separate lane. Unmentioned
voices are never downloaded or transcribed while parked.
"""

from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Optional, Tuple

CLAIM_WINDOW_SECONDS = 120.0
# How long a bare mention waits for a voice from the same /sync batch that is still being gated.
SETTLE_TIMEOUT_SECONDS = 5.0
# Voices a sender can have parked per room/thread at once (oldest by arrival dropped beyond this). Known
# limit: if this many (or more) newer voices park while a bare mention is still settling, the
# older voice it was owed is dropped and the mention is answered as text, as on main.
MAX_PARKED_PER_SENDER = 4

# (voice event_id, content, relates_to)
ParkedVoice = tuple[str, dict, dict]


def has_voice_marker(content: dict) -> bool:
    """The single MSC3245 voice-message check (shared with the adapter's media classifier)."""
    return content.get("org.matrix.msc3245.voice") is not None


def is_voice_event(content: dict) -> bool:
    return content.get("msgtype") == "m.audio" and has_voice_marker(content)


class VoiceGate:
    """One voice being gated; ``seq`` orders voices from the same sender by arrival."""

    __slots__ = ("done", "seq")

    def __init__(self, seq: int) -> None:
        self.seq = seq
        self.done = asyncio.Event()


class ParkedVoices:
    def __init__(self) -> None:
        # (room_id, sender, thread_id) -> parked voices, ordered by seq, bounded; None is room root.
        self._parked: dict[tuple[str, str, Optional[str]], list[tuple[float, int, ParkedVoice]]] = {}
        # Same lane key -> every voice of that sender still being gated. mautrix runs one
        # /sync batch's events as concurrent tasks, so a voice may still be awaiting room
        # identity when its bare mention is handled -- and a sender can have several in flight.
        self._inflight: dict[tuple[str, str, Optional[str]], list[VoiceGate]] = {}
        # Same lane key -> seq of the last claimed voice while gates were in flight, so an
        # older voice finishing late never parks after (and outlives) that claim.
        self._floor: dict[tuple[str, str, Optional[str]], int] = {}
        self._next_seq = 0

    def _prune(self) -> None:
        cutoff = time.monotonic() - CLAIM_WINDOW_SECONDS
        kept = {k: [e for e in v if e[0] >= cutoff] for k, v in self._parked.items()}
        self._parked = {k: v for k, v in kept.items() if v}

    def pending(self, room_id: str, sender: str, *, thread_id: Optional[str] = None) -> bool:
        """Cheap pre-check: an unexpired voice is parked or one is still being gated."""
        key = (room_id, sender, thread_id)
        if key in self._inflight:
            return True
        cutoff = time.monotonic() - CLAIM_WINDOW_SECONDS
        return any(e[0] >= cutoff for e in self._parked.get(key, ()))

    def mark(self) -> int:
        """Arrival limit for a bare mention: only voices that began before this may be claimed."""
        return self._next_seq

    def begin(self, room_id: str, sender: str, *, thread_id: Optional[str] = None) -> VoiceGate:
        """Mark a parkable voice as being gated. Call before the first await; always pair with
        ``release`` (idempotent, so it may run early and again in a ``finally``)."""
        gate = VoiceGate(self._next_seq)
        self._next_seq += 1  # unbounded Python int: never wraps
        self._inflight.setdefault((room_id, sender, thread_id), []).append(gate)
        return gate

    def release(self, room_id: str, sender: str, gate: VoiceGate, *, thread_id: Optional[str] = None) -> None:
        gate.done.set()
        key = (room_id, sender, thread_id)
        gates = self._inflight.get(key)
        if gates and gate in gates:
            gates.remove(gate)
            if not gates:  # no older voice can park any more
                del self._inflight[key]
                self._floor.pop(key, None)

    async def settle(self, room_id: str, sender: str, *, thread_id: Optional[str] = None) -> None:
        """Wait (bounded) for this sender's concurrently gated voices in this lane to park or drop."""
        gates = self._inflight.get((room_id, sender, thread_id))
        if gates:
            waits = [asyncio.ensure_future(g.done.wait()) for g in gates]
            try:
                await asyncio.wait(waits, timeout=SETTLE_TIMEOUT_SECONDS)
            finally:
                for w in waits:
                    w.cancel()

    def park(self, room_id: str, sender: str, gate: VoiceGate, event_id: str, content: dict,
             relates_to: dict, *, thread_id: Optional[str] = None) -> None:
        key = (room_id, sender, thread_id)
        if gate.seq <= self._floor.get(key, -1):
            return  # a newer voice from this sender was already claimed
        self._prune()
        entries = self._parked.setdefault(key, [])
        entries.append((time.monotonic(), gate.seq, (event_id, content, relates_to)))
        entries.sort(key=lambda e: e[1])
        del entries[:-MAX_PARKED_PER_SENDER]

    def claim(self, room_id: str, sender: str, before: int, *, thread_id: Optional[str] = None) -> Optional[ParkedVoice]:
        """Pop the sender's newest parked voice for this lane that began before ``before``
        (``mark()`` taken when the bare mention arrived); older ones are dropped, later voices
        stay for their own mention. The caller re-dispatches it with ``mention_claimed=True``."""
        self._prune()
        key = (room_id, sender, thread_id)
        entries = self._parked.get(key, [])
        idx = max((i for i, e in enumerate(entries) if e[1] < before), default=None)
        if idx is None:
            return None
        claimed = entries[idx]
        del entries[:idx + 1]
        if not entries:
            del self._parked[key]
        if key in self._inflight:  # a late older voice must not park after this claim
            self._floor[key] = max(self._floor.get(key, -1), claimed[1])
        return claimed[2]
