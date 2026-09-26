# Talk to Helix

The iOS and macOS dictation apps first transcribe and clean speech using the
Phoenix dictation backend, then POST the cleaned text directly to
`https://talk-to-helix.undafeed.com/v1/platform-turns/matrix`.
The gateway owns this second step. A 404 here is independent of transcription.

## Contract and configuration

- Requests use the existing dedicated `MATRIX_PLATFORM_TURN_INGRESS_KEY` bearer
  credential. The API-server credential is not interchangeable. Missing, weak,
  or reused credentials leave this route disabled. Never log either credential.
- `gateway.api_server.matrix_platform_turn_allowed_room_id` selects one encrypted
  Matrix room. The existing `MATRIX_PLATFORM_TURN_ALLOWED_ROOM_ID` environment
  override takes precedence; null or malformed room IDs disable ingress.
- The native Matrix adapter, crypto store, profile routing, and session queue own
  each turn. The relay identifies speech as `Andrew (spoken): ...`. HTTP callers
  cannot choose another user, profile, model, or tool configuration.
- JSON requests contain `request_id`, `text`, `tts`, and `audio_format: "wav"`.
  iOS optionally adds `stream: true` or `Accept: text/event-stream`. SSE emits
  provisional `delta`, authoritative `replace`, then `done` (or `error`).
  Both text events carry a JSON object (`data: {"text":"…"}`), never a bare
  JSON string. The iPhone decodes these objects before displaying any answer.
  `done` carries the full JSON success envelope, including optional WAV audio;
  `error` carries `{"code":"…","message":"…"}`.
- Success contains the exact delivered Matrix answer and input/reply event IDs.
  Optional WAV speech uses the established `local-kokoro` command provider with
  `af_jessica` and `mlx-community/Kokoro-82M-bf16`.
- A repeated request ID does not repeat side effects. Completed responses replay
  from a bounded memory cache; persistent fingerprints prevent replaying work
  after a gateway restart. Such an older request can report replay unavailable.
- Disconnecting the app detaches its HTTP wait; the native turn continues.
  A server timeout cancels only that turn and suppresses remaining deliveries.
- The route is not exposed under `/p/<profile>/...`.
- Spoken requests honor `hermes pause`, including already-queued turns.
  Gateway model proxy mode is explicitly unsupported; it is rejected before
  posting the question. The normal HTTP reverse proxy is unaffected.

## Verification and deployment

Run the canonical isolated test runner from the gateway checkout:

```sh
scripts/run_tests.sh -j 4 tests/gateway/test_trusted_matrix_turn.py \
  tests/gateway/test_trusted_matrix_current_gateway.py \
  tests/gateway/test_matrix.py tests/gateway/test_api_server.py \
  tests/gateway/test_api_server_active_work_drain.py \
  tests/gateway/test_busy_wake_admission.py \
  tests/gateway/test_multiplex_api_server_routing.py \
  tests/gateway/test_stream_final_contract.py \
  tests/tools/test_tts_command_providers.py
```

Require an independent review of the current implementation before deployment.
Use the gateway's graceful restart path so existing work can drain. Do not start
another Matrix client against its active crypto store. No native app rebuild or
Phoenix dictation deployment is needed for this gateway repair.

After deployment, and after subsequent gateway upgrades, run:

```sh
scripts/check_talk_to_helix.sh
```

The probe deliberately omits credentials: 401 confirms the route is registered
and rejects unauthenticated callers. It creates no Matrix messages and makes no
model requests. A 404 is a regression (or disabled ingress); a 200 from `/health`
alone cannot prove that Talk to Helix works. Finish with an actual voice request
from each app to verify the configured model, Matrix service, and speech engine.
Gateway streaming tests enforce the native client's text-object contract for
both streaming negotiation paths, including Unicode and embedded newlines.
When changing the wire format, also feed actual HTTP response bytes through
`native_ios/Shared/TalkToHelixClient.swift` in the dictation repository. Testing
only gateway JSON decoding misses incompatibilities with Swift's typed decoder.

## Restoration history

2026-09-09: Ported the preserved August integration to the decomposed gateway:
`run_external_turn` owns ingress orchestration, `run_busy` preserves dedicated
FIFO slots, `run_turn` leaves these turns to native adapter delivery, and
`run_turn_runner` supplies HTTP deltas without duplicate Matrix previews.
Admission receipts and completion after attachment delivery are regression tested.
The running upstream checkout had no Matrix turn route; the older restoration
commits were not ancestors of its revision.

## Why the Studio runs a fork, and how updates work now

The route lived only as local commits on a checkout of `NousResearch/hermes-agent`.
`hermes update` does `git reset --hard origin/main` when the checkout is on `main` and
history has diverged, which silently deleted the feature three times
(2026-08-03, 2026-08-19, 2026-09-22; each time the iPhone showed "HTTP 404").

Since 2026-09-26 the Studio checkout's `origin` is the fork
`git@github.com:anlek/hermes-agent.git`, and the feature is on that fork's `main`.
`upstream` is `NousResearch/hermes-agent`. Consequences:

- `hermes update` fast-forwards from the fork's `main`. It can no longer remove the
  feature, because the feature *is* origin/main. When the fork is ahead of upstream the
  updater prints "Your fork has N commit(s) not on upstream. Skipping upstream sync";
  that line is expected.
- Upstream changes reach the fork only through `scripts/sync_upstream_fork.sh`
  (Hermes cron job "Hermes fork sync", daily, no-agent). It merges `upstream/main` into
  the fork's `main` in the worktree `~/code/helix/hermes-fork-sync`, runs the Talk to
  Helix tests, and pushes only if they pass. A conflict or test failure changes nothing
  and posts a notice to Matrix; the merge is then finished by hand on branch `fork-sync`.
- After a successful sync, deploy as usual: `hermes update` (or the `/update` command),
  which restarts the gateway.
- The Hermes cron job "Talk to Helix route watchdog" (hourly, no-agent) runs
  `scripts/check_talk_to_helix.sh` and posts to Matrix only when the route stops
  answering 401 to an unauthenticated request, or when the last fork sync failed.

### If the route ever 404s again

```sh
cd ~/code/helix/hermes-agent
git remote -v                 # origin MUST be github.com/anlek/hermes-agent
git branch --show-current     # MUST be main
git log --oneline -1 origin/main
git ls-tree HEAD gateway/trusted_matrix_turn.py   # empty => feature missing from HEAD
git fetch origin && git checkout main && git reset --hard origin/main
launchctl kickstart -k gui/$(id -u)/ai.hermes.gateway
scripts/check_talk_to_helix.sh   # expects HTTP 401
```

If `origin` points at NousResearch again, someone re-cloned or edited remotes:
`git remote set-url origin git@github.com:anlek/hermes-agent.git` and repeat the steps.
The Studio pushes to the fork as the GitHub user `helix-anlek` (collaborator with push).
