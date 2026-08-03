"""Deterministic identifiers and source URLs for upload ingestion."""

import uuid


_INGESTION_JOB_NAMESPACE = uuid.UUID("35f1bb68-928d-4f1f-9f63-8ac8c176c4df")


def minio_source_url(bucket: str, object_name: str) -> str:
    """Return the canonical source URL used by transcoder retry paths."""
    return f"minio://{bucket.strip('/')}/{object_name.lstrip('/')}"


def ingestion_job_id(video_id: str, bucket: str, object_name: str) -> str:
    """Return one stable Job ID for a stored upload object.

    RabbitMQ can redeliver ``upload.completed`` messages. Deriving the Job ID
    from immutable object identity makes those deliveries converge on the same
    database row instead of starting independent ingestion attempts.
    """
    identity = "\n".join((str(video_id), str(bucket), str(object_name)))
    return str(uuid.uuid5(_INGESTION_JOB_NAMESPACE, identity))
