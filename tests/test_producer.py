"""Unit tests for Kafka source rate shaping and event production helpers."""
from src.producer import RateProfile

def test_rate_profile_starts_with_peak():
    profile = RateProfile(base_rate=5, peak_rate=15, peak_every_s=60, peak_duration_s=10)
    assert profile.rate_at(0) == 15
    assert profile.rate_at(9.9) == 15
    assert profile.rate_at(10) == 5
    assert profile.rate_at(60) == 15
