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

class FifoDeduplicationCalculator:
    def __init__(self, message_group_id: str):
        self.message_group_id = message_group_id

    def generate_dedup_hash(self, body_payload: Dict[str, Any]) -> str:
        logger.info(f"Calculating deterministic deduplication hash for group {self.message_group_id}")

        # Input validation: ensure body_payload is a dict
        if not isinstance(body_payload, dict):
            raise ValueError(f"body_payload must be a dict, got {type(body_payload).__name__}")

        components: Set[str] = set()
        components.add(str(body_payload.get("transaction_id")))
        components.add(str(body_payload.get("timestamp")))

        # Upstream service passes nested context tags as a dict.
        # Dicts are unhashable and cannot be added to a set directly.
        # Serialize to a deterministic JSON string (sort_keys=True ensures
        # key-order independence) so the set membership check is stable.
        client_context = body_payload.get("client_context", {})
        if isinstance(client_context, dict):
            components.add(json.dumps(client_context, sort_keys=True))
        elif client_context is not None:
            # Gracefully handle unexpected non-dict, non-None types
            logger.warning(
                f"client_context has unexpected type {type(client_context).__name__}; "
                "coercing to string for hashing"
            )
            components.add(str(client_context))
        # If client_context is None, we simply omit it from the component pool

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
        logger.error(
            f"Failed to generate deduplication hash for payload: {exc}",
            exc_info=True
        )
        return {
            "statusCode": 400,
            "error": "DeduplicationHashGenerationFailed",
            "detail": str(exc)
        }

    logger.info(f"Generated DeduplicationId: {dedup_id}")

    sqs_message = {
        "MessageBody": json.dumps(event_payload),
        "MessageGroupId": "PAYMENT_ROUTING",
        "MessageDeduplicationId": dedup_id
    }

    return {"statusCode": 200, "sqs_message": sqs_message}


if __name__ == "__main__":
    lambda_handler({}, None)
