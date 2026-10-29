"""A test process never resolves the live Hermes home, wherever the test file lives."""

from pathlib import Path

import pytest

import hermes_constants
from hermes_constants import get_default_hermes_root, get_hermes_home


@pytest.fixture
def live_root(tmp_path, monkeypatch):
    """Stand in tmp_path for the OS account home, so the account's live root is tmp_path/.hermes."""
    monkeypatch.setattr(hermes_constants, "_account_home", lambda: tmp_path)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(hermes_constants, "_LIVE_HERMES_ROOTS", None)
    monkeypatch.setattr(hermes_constants, "_default_hermes_root_memo", None)
    for name in ("HERMES_DATA_DIR_SUFFIX", "HERMES_LIVE_HOME_GUARD_BYPASS", "HERMES_STATE_DB_GUARD_BYPASS"):
        monkeypatch.delenv(name, raising=False)
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
    with pytest.raises(RuntimeError, match="live Hermes home"):
        get_default_hermes_root()


@pytest.mark.platforms("linux", "macos")
def test_other_homes_resolve(live_root, monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "sandbox"))
    assert get_hermes_home() == tmp_path / "sandbox"


@pytest.mark.platforms("linux", "macos")
@pytest.mark.parametrize("bypass", ["HERMES_LIVE_HOME_GUARD_BYPASS", "HERMES_STATE_DB_GUARD_BYPASS"])
def test_bypass_lets_a_deliberate_test_through(live_root, monkeypatch, bypass):
    monkeypatch.delenv("HERMES_HOME", raising=False)
    monkeypatch.setenv(bypass, "1")
    assert get_hermes_home() == live_root
