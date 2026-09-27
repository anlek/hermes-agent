#!/bin/sh
# Merge NousResearch/hermes-agent main into the anlek/hermes-agent fork's main,
# but only when the Talk to Helix gateway tests still pass afterwards.
#
# Why this exists: the Talk to Helix route (POST /v1/platform-turns/matrix) is a
# Studio-local feature. `hermes update` on a checkout of the upstream repo does
# `git reset --hard origin/main` when history diverges, which silently deleted
# the feature three times (2026-08-03, 2026-08-19, 2026-09-22). The Studio's
# origin is therefore the fork; upstream only reaches main through this script,
# and only with the feature's tests green. A failed merge changes nothing.
#
# Runs from a dedicated worktree so the live gateway checkout is never touched.
set -eu

FORK_REMOTE=${FORK_REMOTE:-origin}
UPSTREAM_REMOTE=${UPSTREAM_REMOTE:-upstream}
UPSTREAM_URL=${UPSTREAM_URL:-https://github.com/NousResearch/hermes-agent.git}
LIVE_CHECKOUT=${LIVE_CHECKOUT:-/Users/hermesagent/code/helix/hermes-agent}
SYNC_WORKTREE=${SYNC_WORKTREE:-/Users/hermesagent/code/helix/hermes-fork-sync}
STATUS_FILE=${STATUS_FILE:-/Users/hermesagent/.hermes/logs/fork-sync.status}
LOG_TAG="fork-sync $(date '+%Y-%m-%d %H:%M:%S')"

# test_trusted_matrix_current_gateway.py is omitted: it needs mautrix, which macOS
# installs do not ship; run it by hand with a scratch mautrix when touching the seam.
TESTS="tests/gateway/test_trusted_matrix_turn.py \
tests/gateway/test_matrix.py \
tests/gateway/test_api_server.py \
tests/tools/test_tts_command_providers.py"

# OK is silent on stdout (Hermes cron --no-agent delivers stdout only when non-empty),
# so only failures reach the Matrix room. Every run is recorded in STATUS_FILE and stderr.
status() { printf '%s %s %s\n' "$(date '+%Y-%m-%dT%H:%M:%S')" "$1" "$2" > "$STATUS_FILE"; echo "$LOG_TAG: $1 $2" >&2; }
fail() { status FAILED "$1"; echo "hermes fork sync FAILED: $1"; exit 1; }

cd "$LIVE_CHECKOUT"
git remote get-url "$UPSTREAM_REMOTE" >/dev/null 2>&1 || git remote add "$UPSTREAM_REMOTE" "$UPSTREAM_URL"
git fetch --quiet "$FORK_REMOTE" main || fail "fetch $FORK_REMOTE failed"
git fetch --quiet "$UPSTREAM_REMOTE" main || fail "fetch $UPSTREAM_REMOTE failed"

if [ ! -d "$SYNC_WORKTREE" ]; then
  git worktree add --quiet -B fork-sync "$SYNC_WORKTREE" "$FORK_REMOTE/main" || fail "worktree add failed"
fi
cd "$SYNC_WORKTREE"
git merge --abort >/dev/null 2>&1 || true
git checkout --quiet -B fork-sync "$FORK_REMOTE/main"

if git merge-base --is-ancestor "$UPSTREAM_REMOTE/main" HEAD; then
  status OK "fork main already contains upstream main ($(git rev-parse --short "$UPSTREAM_REMOTE/main"))"
  exit 0
fi

if ! git -c user.name="fork-sync" -c user.email="helix@eightstory.com" \
     merge --no-edit --no-ff "$UPSTREAM_REMOTE/main" \
     -m "Merge upstream main $(git rev-parse --short "$UPSTREAM_REMOTE/main") into fork (Talk to Helix kept)"; then
  conflicts=$(git diff --name-only --diff-filter=U | tr '\n' ' ')
  git merge --abort || true
  fail "merge conflict with upstream in: $conflicts -- resolve by hand on branch fork-sync, then push $FORK_REMOTE main"
fi

for f in gateway/trusted_matrix_turn.py scripts/check_talk_to_helix.sh; do
  [ -f "$f" ] || fail "$f missing after merge; feature lost"
done

ulimit -n 10240 2>/dev/null || true   # macOS default 256 fds breaks the api_server tests
if ! scripts/run_tests.sh -j 4 $TESTS > "$SYNC_WORKTREE/.fork-sync-tests.log" 2>&1; then
  tail -20 "$SYNC_WORKTREE/.fork-sync-tests.log" || true
  fail "Talk to Helix tests failed after merging upstream; see $SYNC_WORKTREE/.fork-sync-tests.log (branch fork-sync kept for repair)"
fi

git push --quiet "$FORK_REMOTE" fork-sync:main || fail "push to $FORK_REMOTE main failed"
status OK "merged upstream $(git rev-parse --short "$UPSTREAM_REMOTE/main") into fork main $(git rev-parse --short HEAD); run 'hermes update' to deploy"
