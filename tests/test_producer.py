import pytest

from src.producer import publish_events


def test_publish_rejects_zero_events():
    with pytest.raises(ValueError, match="at least 1"):
        publish_events(0)
