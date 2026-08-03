"""RabbitMQ event publishing helpers (used outside Celery's own broker)."""

import json
import logging

from kombu import Connection, Exchange, Producer

from app.config import get_settings

logger = logging.getLogger(__name__)


def publish_event(event_type: str, payload: dict) -> None:
    """Publish a domain event to the configured RabbitMQ topic exchange."""
    settings = get_settings()
    try:
        body = json.dumps({"type": event_type, "payload": payload}).encode("utf-8")
        with Connection(settings.RABBITMQ_URL) as conn:
            exchange = Exchange(settings.RABBITMQ_EXCHANGE, type="topic", durable=True)
            with Producer(conn, exchange=exchange, routing_key=event_type) as producer:
                producer.publish(
                    body,
                    content_type="application/json",
                    declare=[exchange],
                )
        logger.debug("Published event %s", event_type)
    except Exception:
        logger.exception("Failed to publish event %s", event_type)
