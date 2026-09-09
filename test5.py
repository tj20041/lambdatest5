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

        try:
            components: Set[str] = set()
            components.add(str(body_payload.get("transaction_id")))
            components.add(str(body_payload.get("timestamp")))

            # Upstream service passes nested context tags
            tags = body_payload.get("client_context", {})

            # tags may be a dict (or other unhashable structure such as a list).
            # Convert it to a deterministic, hashable string representation using
            # canonical JSON serialization (sorted keys) before adding it to the
            # set, instead of inserting the raw unhashable object directly.
            if isinstance(tags, (dict, list)):
                tags_component = json.dumps(tags, sort_keys=True)
            else:
                tags_component = str(tags)
            components.add(tags_component)

            serialized = "".join(sorted(components))
            return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        except TypeError as exc:
            logger.exception(
                f"Failed to generate deduplication hash for group {self.message_group_id} "
                f"due to an unhashable or unsupported payload structure: {exc}"
            )
            raise TypeError(
                f"Unable to compute deduplication hash: payload contains an unsupported "
                f"or unhashable structure. Original error: {exc}"
            ) from exc

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
    except TypeError as exc:
        logger.exception(f"Deduplication hash generation failed for payload: {event_payload}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "Failed to generate deduplication hash",
                "details": str(exc)
            })
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
