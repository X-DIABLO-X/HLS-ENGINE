"""Standalone Intel Quick Sync proof-of-concept worker.

This package is intentionally not imported by the Celery application.  It is a
small Windows-native adapter that can be exercised independently while the
production transcoding path remains unchanged.
"""

from .adapter import (
    EncodeJob,
    EncodingFailedError,
    JobConfigurationError,
    ProcessResult,
    QsvAdapter,
    QsvUnavailableError,
    QsvWorkerError,
    RenditionSettings,
    SourceRange,
)

__all__ = [
    "EncodeJob",
    "EncodingFailedError",
    "JobConfigurationError",
    "ProcessResult",
    "QsvAdapter",
    "QsvUnavailableError",
    "QsvWorkerError",
    "RenditionSettings",
    "SourceRange",
]
