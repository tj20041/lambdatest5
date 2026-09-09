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
        
        components: Set[Any] = set()
        components.add(body_payload.get("transaction_id"))
        components.add(body_payload.get("timestamp"))

        # Upstream service passes nested context tags
        tags = body_payload.get("client_context", {})

        # FAILS HERE: tags is a dict: {'ip': '10.0.0.1', 'region': 'us-east-1'}
        # Python sets cannot contain unhashable types (dicts)
        # Raises TypeError: unhashable type: 'dict'
        components.add(tags)

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
    dedup_id = calculator.generate_dedup_hash(event_payload)
    logger.info(f"Generated DeduplicationId: {dedup_id}")

    sqs_message = {
        "MessageBody": json.dumps(event_payload),
        "MessageGroupId": "PAYMENT_ROUTING",
        "MessageDeduplicationId": dedup_id
    }

    return {"statusCode": 200, "sqs_message": sqs_message}

if __name__ == "__main__":
    lambda_handler({}, None)
