"""Smoke test: proves the test harness and package import wiring work."""

from producer import __version__


def test_producer_version_is_exposed() -> None:
    assert __version__ == "0.1.0"
