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

    def generate_dedup_hash(self, body_payload: Dict[str, Any]) -> str:
        logger.info(f"Calculating deterministic deduplication hash for group {self.message_group_id}")

        components: Set[str] = set()
        components.add(str(body_payload.get("transaction_id")))
        components.add(str(body_payload.get("timestamp")))

        # Upstream service passes nested context tags as a dict.
        # Serialize to a stable JSON string (sort_keys=True ensures determinism)
        # so the dict can be safely added to the set as a hashable str.
        client_context = body_payload.get("client_context", {})
        if isinstance(client_context, dict):
            tags_serialized = json.dumps(client_context, sort_keys=True)
        else:
            tags_serialized = str(client_context)
        components.add(tags_serialized)

        serialized = "".join(sorted(components))
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
            f"Failed to generate deduplication hash for group "
            f"{calculator.message_group_id}: {exc}",
            exc_info=True
        )
        # Fallback: hash only transaction_id + timestamp to avoid total invocation failure
        fallback_raw = str(event_payload.get("transaction_id")) + str(event_payload.get("timestamp"))
        dedup_id = hashlib.sha256(fallback_raw.encode("utf-8")).hexdigest()
        logger.warning(f"Using fallback deduplication hash: {dedup_id}")

    logger.info(f"Generated DeduplicationId: {dedup_id}")

    sqs_message = {
        "MessageBody": json.dumps(event_payload),
        "MessageGroupId": "PAYMENT_ROUTING",
        "MessageDeduplicationId": dedup_id
    }

    return {"statusCode": 200, "sqs_message": sqs_message}

if __name__ == "__main__":
    lambda_handler({}, None)
