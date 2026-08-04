import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError

from app.celery_app import celery_app
from app.config import Settings
from app.progress import DEFAULT_SETTINGS


REPO_ROOT = Path(__file__).resolve().parents[3]


class ResilienceConfigTests(unittest.TestCase):
    def test_safe_defaults_use_audited_single_pass_encoding(self):
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings(_env_file=None)

        self.assertFalse(settings.CHUNKED_ENCODING)
        self.assertFalse(DEFAULT_SETTINGS["chunked_encoding"])
        self.assertEqual(settings.CHUNK_MIN_DURATION_SEC, 600)
        self.assertLessEqual(settings.CHUNK_DURATION_SEC, 300)

        example_env = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
        self.assertRegex(example_env, r"(?m)^CHUNKED_ENCODING=false$")

    def test_chunk_duration_rejects_unbounded_values(self):
        with self.assertRaises(ValidationError):
            Settings(_env_file=None, CHUNK_DURATION_SEC=301)

    def test_soft_task_limit_must_precede_hard_limit(self):
        with self.assertRaises(ValidationError):
            Settings(
                _env_file=None,
                CELERY_TASK_SOFT_TIME_LIMIT_SEC=7200,
                CELERY_TASK_TIME_LIMIT_SEC=7200,
            )

    def test_broker_timeout_must_exceed_hard_limit_plus_margin(self):
        with self.assertRaises(ValidationError):
            Settings(
                _env_file=None,
                CELERY_TASK_SOFT_TIME_LIMIT_SEC=1700,
                CELERY_TASK_TIME_LIMIT_SEC=1800,
                CELERY_ACK_TIMEOUT_MARGIN_SEC=900,
                RABBITMQ_CONSUMER_TIMEOUT_MS=1_800_000,
            )

    def test_celery_cancels_unacknowledgeable_work_on_connection_loss(self):
        self.assertTrue(
            celery_app.conf.worker_cancel_long_running_tasks_on_connection_loss
        )
        self.assertFalse(celery_app.conf.worker_enable_remote_control)
        self.assertLess(
            celery_app.conf.task_soft_time_limit,
            celery_app.conf.task_time_limit,
        )

    def test_workspace_reaper_retry_horizon_covers_hard_task_death(self):
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings(_env_file=None)

        self.assertTrue(settings.WORKSPACE_REAPER_ENABLED)
        self.assertGreaterEqual(
            settings.WORKSPACE_CLEANUP_RETRY_SEC
            * settings.WORKSPACE_CLEANUP_MAX_RETRIES,
            settings.CELERY_TASK_TIME_LIMIT_SEC
            + settings.WORKSPACE_CLEANUP_GRACE_SEC,
        )
        with self.assertRaises(ValidationError):
            Settings(
                _env_file=None,
                WORKSPACE_CLEANUP_RETRY_SEC=1,
                WORKSPACE_CLEANUP_MAX_RETRIES=1,
            )

        compose = (REPO_ROOT / "docker-compose.yml").read_text(
            encoding="utf-8"
        )
        self.assertEqual(
            compose.count(
                "WORKSPACE_REAPER_ENABLED: "
                "${WORKSPACE_REAPER_ENABLED:-true}"
            ),
            4,
        )
        self.assertIn("hls-work:/tmp/hls-work:ro", compose)
        self.assertEqual(
            celery_app.conf.task_routes[
                "app.tasks.workspace_cleanup.*"
            ]["queue"],
            "package",
        )

    def test_broker_timeout_default_exceeds_celery_hard_limit(self):
        compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        match = re.search(
            r"RABBITMQ_CONSUMER_TIMEOUT_MS: "
            r"\$\{RABBITMQ_CONSUMER_TIMEOUT_MS:-(\d+)\}",
            compose,
        )
        self.assertIsNotNone(match)

        broker_timeout_ms = int(match.group(1))
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings(_env_file=None)

        self.assertGreater(
            broker_timeout_ms,
            (
                settings.CELERY_TASK_TIME_LIMIT_SEC
                + settings.CELERY_ACK_TIMEOUT_MARGIN_SEC
            )
            * 1000,
        )
        rabbit_config = (
            REPO_ROOT
            / "infra"
            / "rabbitmq"
            / "conf.d"
            / "90-consumer-timeout.conf"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "consumer_timeout = $(RABBITMQ_CONSUMER_TIMEOUT_MS)",
            rabbit_config,
        )

    def test_compose_defaults_every_pipeline_process_to_single_pass(self):
        compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertEqual(
            compose.count("CHUNKED_ENCODING: ${CHUNKED_ENCODING:-false}"),
            4,
        )
        self.assertNotIn(
            "CHUNKED_ENCODING: ${CHUNKED_ENCODING:-true}",
            compose,
        )
        self.assertGreaterEqual(compose.count("init: true"), 2)

    def test_compose_exposes_snapshotted_media_switches(self):
        compose = (REPO_ROOT / "docker-compose.yml").read_text(
            encoding="utf-8"
        )

        self.assertEqual(
            compose.count("NVENC_PROFILE: ${NVENC_PROFILE:-}"),
            4,
        )
        self.assertEqual(
            compose.count(
                "AAC_PASSTHROUGH_ENABLED: "
                "${AAC_PASSTHROUGH_ENABLED:-false}"
            ),
            3,
        )

    def test_worker_execs_celery_and_does_not_detach_heartbeat(self):
        wrapper = (
            REPO_ROOT / "services" / "transcoder" / "celery-worker-start.sh"
        ).read_text(encoding="utf-8")

        self.assertIn('exec "$CELERY_BIN" -A app.celery_app worker', wrapper)
        self.assertNotIn("nohup python", wrapper)
        self.assertNotIn("celery -A app.celery_app worker &", wrapper)
        self.assertIn("gpu_probe=detect_gpu", wrapper)
        self.assertIn("GPU_REGISTRATION_STOP_FILE", wrapper)
        self.assertNotIn("gpu_probe=is_gpu_available", wrapper)
        self.assertIn("--without-mingle", wrapper)
        self.assertIn("--without-gossip", wrapper)

        compose = (REPO_ROOT / "docker-compose.yml").read_text(
            encoding="utf-8"
        )
        self.assertEqual(
            compose.count(
                'test: ["CMD", "python", "-m", "app.worker_health"]'
            ),
            2,
        )
        self.assertNotIn("celery -A app.celery_app inspect ping", compose)

    @unittest.skipUnless(shutil.which("bash"), "worker wrapper targets Linux containers")
    def test_worker_exit_is_not_held_open_by_gpu_heartbeat(self):
        wrapper = (
            REPO_ROOT / "services" / "transcoder" / "celery-worker-start.sh"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            fake_celery = Path(temp_dir) / "fake-celery"
            fake_celery.write_text("#!/bin/sh\nexit 23\n", encoding="utf-8")
            fake_celery.chmod(0o755)

            env = os.environ.copy()
            env.update(
                {
                    "CELERY_BIN": str(fake_celery),
                    "GPU_ENABLED": "false",
                    "GPU_WORKER_ROLE": "true",
                    "PYTHONPATH": str(REPO_ROOT / "services" / "transcoder"),
                }
            )
            result = subprocess.run(
                ["bash", str(wrapper)],
                cwd=REPO_ROOT / "services" / "transcoder",
                env=env,
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )

        self.assertEqual(result.returncode, 23, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
