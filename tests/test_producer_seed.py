"""Seed choice in the producer CLI."""

from unittest.mock import patch

import src.kafka_producer as producer


def run(argv):
    seeds = []
    with (
        patch.object(producer, "publish_events", lambda **kw: seeds.append(kw["seed"])),
        patch("signal.signal"),
    ):
        producer.main(argv)
    return seeds[0]


def test_continuous_runs_get_a_new_seed_each_time():
    assert run(["--continuous"]) != run(["--continuous"])


def test_batch_default_and_explicit_seed_stay_reproducible():
    assert run([]) == 42
    assert run(["--continuous", "--seed", "7"]) == 7
