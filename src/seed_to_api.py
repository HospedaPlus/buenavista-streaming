"""Send synthetic seed events through FastAPI instead of bypassing the API.

Example:
    python -m src.seed_to_api --count 100 --api-url http://localhost:8000
"""

import argparse
import json
import logging
from itertools import islice
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.events import generate_events

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def send_seed_events(api_url: str, count: int, seed: int) -> int:
    """Generate a bounded synthetic event batch and POST each event to FastAPI."""
    if count < 1:
        raise ValueError("count must be at least 1")

    endpoint = f"{api_url.rstrip('/')}/events"
    sent = 0
    for event in islice(generate_events(seed=seed), count):
        body = event.model_dump_json().encode("utf-8")
        request = Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=15) as response:
                result = json.loads(response.read().decode("utf-8"))
                if response.status != 202:
                    raise RuntimeError(f"API returned unexpected status {response.status}")
                logger.info(
                    "Event accepted by API: %s (%s/%s)",
                    result["event_id"],
                    sent + 1,
                    count,
                )
                sent += 1
        except (HTTPError, URLError, TimeoutError) as exc:
            logger.exception("Stopped at event %s; API did not confirm acceptance", sent + 1)
            raise RuntimeError(
                "Seed ingestion stopped. Do not assume the failed event reached Kafka."
            ) from exc

    logger.info("Seed ingestion complete: accepted=%s endpoint=%s", sent, endpoint)
    return sent


def main() -> None:
    """Parse CLI arguments for a reproducible, finite seed run."""
    parser = argparse.ArgumentParser(description="Send synthetic seed events through FastAPI.")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--api-url", default="http://localhost:8000")
    args = parser.parse_args()
    send_seed_events(args.api_url, args.count, args.seed)


if __name__ == "__main__":
    main()
