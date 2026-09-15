import hashlib
import json
import logging
import sys
import time
from typing import Any, Dict, List, Set

logger = logging.getLogger("fifo_dedup_engine")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s"))
logger.handlers = [stream_handler]


def _to_hashable(value: Any) -> Any:
    """
    Recursively convert a value into a hashable representation so it can
    safely be added to a Python set().

    - dict -> deterministic JSON string (sort_keys=True) so ordering does
      not affect the resulting hash.
    - list/tuple/set -> tuple of recursively converted elements.
    - Everything else (str, int, float, bool, None, etc.) is returned as-is
      since it is already hashable.
    """
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, default=str)
    if isinstance(value, (list, tuple, set)):
        return tuple(_to_hashable(item) for item in value)
    return value


class FifoDeduplicationCalculator:
    def __init__(self, message_group_id: str):
        self.message_group_id = message_group_id

    def generate_dedup_hash(self, body_payload: Dict[str, Any]) -> str:
        logger.info(f"Calculating deterministic deduplication hash for group {self.message_group_id}")

        # Only hashable primitives or JSON-serialized strings (via
        # _to_hashable) should ever be added to this set.
        components: Set[Any] = set()
        components.add(_to_hashable(body_payload.get("transaction_id")))
        components.add(_to_hashable(body_payload.get("timestamp")))

        # Upstream service passes nested context tags
        tags = body_payload.get("client_context", {})

        # tags may be a dict (e.g. {'ip': '10.0.0.1', 'region': 'us-east-1'}).
        # Python sets cannot contain unhashable types (dicts), so we convert
        # it to a deterministic, order-independent JSON string first.
        components.add(_to_hashable(tags) if tags else None)

        serialized = "".join(sorted([str(c) for c in components]))
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("Processing inbound outbound queue dispatcher event...")

    event_payload = {
        "transaction_id": "TXN_88192031",
        "timestamp": 1716002010,
        "amount": 2500.00,
        "client_context": {
            "source_ip": "10.0.4.15",
            "environment": "production"
        }
    }

    calculator = FifoDeduplicationCalculator(message_group_id="PAYMENT_ROUTING")

    try:
        dedup_id = calculator.generate_dedup_hash(event_payload)
    except (TypeError, ValueError) as exc:
        logger.error(f"Failed to generate deduplication hash: {exc}", exc_info=True)
        return {"statusCode": 500, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - top-level guard for Lambda invocation
        logger.error(f"Unexpected error generating deduplication hash: {exc}", exc_info=True)
        return {"statusCode": 500, "error": str(exc)}

    logger.info(f"Generated DeduplicationId: {dedup_id}")

    sqs_message = {
        "MessageBody": json.dumps(event_payload),
        "MessageGroupId": "PAYMENT_ROUTING",
        "MessageDeduplicationId": dedup_id
    }

    return {"statusCode": 200, "sqs_message": sqs_message}


if __name__ == "__main__":
    lambda_handler({}, None)
