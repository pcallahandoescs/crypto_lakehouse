"""Smoke test: proves the test harness and package import wiring work.

Real unit tests for transformations and data-quality logic arrive in Week 3
(Day 21). Until then this keeps `make check` green and CI meaningful.
"""

from producer import __version__


def test_producer_version_is_exposed() -> None:
    assert __version__ == "0.1.0"
