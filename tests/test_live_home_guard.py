"""A test process never resolves the live Hermes home, wherever the test file lives."""

from pathlib import Path

import pytest

import hermes_constants
from hermes_constants import get_hermes_home


@pytest.fixture
def live_root(tmp_path, monkeypatch):
    """Stand in tmp_path for the OS account home, so the account's live root is tmp_path/.hermes."""
    monkeypatch.setattr(hermes_constants, "_account_home", lambda: tmp_path)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(hermes_constants, "_LIVE_HERMES_ROOT", None)
    monkeypatch.delenv("HERMES_DATA_DIR_SUFFIX", raising=False)
    return tmp_path / ".hermes"


@pytest.mark.platforms("linux", "macos")
@pytest.mark.parametrize("env_home", [None, "", "profiles/forge"])
def test_live_home_and_its_profiles_are_refused(live_root, monkeypatch, env_home):
    if env_home is None:
        monkeypatch.delenv("HERMES_HOME", raising=False)
    else:
        monkeypatch.setenv("HERMES_HOME", str(live_root / env_home))
    with pytest.raises(RuntimeError, match="live Hermes home"):
        get_hermes_home()


@pytest.mark.platforms("linux", "macos")
def test_other_homes_resolve(live_root, monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "sandbox"))
    assert get_hermes_home() == tmp_path / "sandbox"


@pytest.mark.platforms("linux", "macos")
def test_the_live_db_guard_bypass_also_bypasses_this(live_root, monkeypatch):
    monkeypatch.delenv("HERMES_HOME", raising=False)
    monkeypatch.setenv("HERMES_STATE_DB_GUARD_BYPASS", "1")
    assert get_hermes_home() == live_root
