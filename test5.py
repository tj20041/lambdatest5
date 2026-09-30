import hashlib
import json
import logging
import sys
import time
from typing import Any, Dict, Set

logger = logging.getLogger("fifo_dedup_engine")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s"))
logger.handlers = [stream_handler]


class FifoDeduplicationCalculator:
    def __init__(self, message_group_id: str):
        self.message_group_id = message_group_id

    @staticmethod
    def _to_hashable(value: Any) -> Any:
        """
        Recursively converts a value into a hashable representation so it can
        safely be inserted into a Python set().

        - dict  -> JSON string (keys sorted for determinism)
        - list  -> tuple of recursively converted elements
        - set   -> frozenset of recursively converted elements
        - Other -> returned as-is (str, int, float, None are already hashable)
        """
        if isinstance(value, dict):
            return json.dumps(value, sort_keys=True, default=str)
        if isinstance(value, list):
            return tuple(FifoDeduplicationCalculator._to_hashable(v) for v in value)
        if isinstance(value, set):
            return frozenset(FifoDeduplicationCalculator._to_hashable(v) for v in value)
        return value

    def generate_dedup_hash(self, body_payload: Dict[str, Any]) -> str:
        logger.info(
            f"Calculating deterministic deduplication hash for group {self.message_group_id}"
        )

        components: Set[Any] = set()

        # Normalise every field to a hashable type before adding to the set
        components.add(self._to_hashable(body_payload.get("transaction_id")))
        components.add(self._to_hashable(body_payload.get("timestamp")))

        # client_context is a nested dict; serialise it to a deterministic JSON
        # string so it can be stored in the set without raising TypeError.
        tags = body_payload.get("client_context", {})
        components.add(self._to_hashable(tags))

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
            "environment": "production",
        },
    }

    calculator = FifoDeduplicationCalculator(message_group_id="PAYMENT_ROUTING")

    try:
        dedup_id = calculator.generate_dedup_hash(event_payload)
    except (TypeError, ValueError) as exc:
        logger.error(
            f"Failed to generate deduplication hash for payload "
            f"{json.dumps(event_payload, default=str)}: {exc}"
        )
        raise

    logger.info(f"Generated DeduplicationId: {dedup_id}")

    sqs_message = {
        "MessageBody": json.dumps(event_payload),
        "MessageGroupId": "PAYMENT_ROUTING",
        "MessageDeduplicationId": dedup_id,
    }

    return {"statusCode": 200, "sqs_message": sqs_message}


if __name__ == "__main__":
    lambda_handler({}, None)
