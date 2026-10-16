"""Unit tests for producer input validation."""

import pytest

from src.producer import RateProfile, ThroughputMeter, publish_events


def test_publish_rejects_zero_events():
    """Publishing zero or a negative number of events should fail immediately."""
    with pytest.raises(ValueError, match="at least 1"):
        publish_events(0)


def test_publish_rejects_non_positive_duration():
    with pytest.raises(ValueError, match="duration"):
        publish_events(count=None, duration_s=0)


def test_rate_profile_starts_with_peak_then_base():
    """Each cycle opens with its peak window and then drops to the base rate."""
    profile = RateProfile(base_rate=5, peak_rate=15, peak_every_s=60, peak_duration_s=10)
    assert profile.rate_at(0) == 15
    assert profile.rate_at(9.9) == 15
    assert profile.rate_at(10) == 5
    assert profile.rate_at(59) == 5
    assert profile.rate_at(61) == 15


def test_default_profile_meets_f2_peak_requirement():
    """F2 requires at least 10 events per second at peak."""
    assert RateProfile().peak_rate >= 10


def test_rate_profile_without_peaks_uses_base_rate():
    profile = RateProfile(base_rate=3, peak_rate=20, peak_every_s=0)
    assert profile.rate_at(0) == 3


def test_rate_profile_rejects_invalid_rates():
    with pytest.raises(ValueError):
        RateProfile(base_rate=0)


def test_throughput_meter_tracks_peak_second():
    meter = ThroughputMeter(started_at=0.0)
    for timestamp in (0.1, 0.5, 0.9, 1.2, 2.1, 2.2, 2.3, 2.4, 3.0):
        meter.record(now=timestamp)
    assert meter.completed_seconds == [3, 1, 4]
    assert meter.peak_per_second == 4
    assert meter.total == 9


def test_throughput_meter_counts_idle_seconds_as_zero():
    meter = ThroughputMeter(started_at=0.0)
    meter.record(now=0.5)
    meter.record(now=3.5)
    assert meter.completed_seconds == [1, 0, 0]
