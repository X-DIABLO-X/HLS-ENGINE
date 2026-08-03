import ast
from pathlib import Path
import unittest
from unittest.mock import patch

from app import main, progress


class _GenerationRedis:
    def __init__(self, job_id: str, updated_at: str):
        self.data = {
            "job_id": job_id,
            "updated_at": updated_at,
        }

    def eval(self, script, _num_keys, _key, *args):
        requested_job = str(args[0])
        current_job = str(self.data.get("job_id") or "")
        if current_job and (
            not requested_job or current_job != requested_job
        ):
            return 0
        if requested_job and not current_job:
            return 0
        for index in range(1, len(args), 2):
            self.data[str(args[index])] = str(args[index + 1])
        return 1

    def hget(self, _key, field):
        value = self.data.get(field)
        return value.encode() if value is not None else None


class ProgressGenerationTests(unittest.TestCase):
    def test_superseded_callback_cannot_refresh_current_updated_at(self):
        client = _GenerationRedis("job-new", "200.0")
        with patch.object(progress, "_redis", return_value=client):
            progress.update_task(
                "video-1",
                "transcode_720p",
                91,
                "old encoder",
                job_id="job-old",
            )

        self.assertEqual(client.data["updated_at"], "200.0")
        self.assertNotIn("task:transcode_720p", client.data)

    def test_missing_generation_cannot_bypass_active_fence(self):
        client = _GenerationRedis("job-new", "200.0")
        with patch.object(progress, "_redis", return_value=client):
            progress.update_task(
                "video-1",
                "audio_eng",
                91,
                "legacy callback",
            )

        self.assertEqual(client.data["updated_at"], "200.0")
        self.assertNotIn("task:audio_eng", client.data)

    def test_legacy_hash_without_generation_remains_mutable(self):
        client = _GenerationRedis("", "100.0")
        with patch.object(progress, "_redis", return_value=client):
            progress.set_percent("video-legacy", 50, "legacy")

        self.assertEqual(client.data["percent"], "50")
        self.assertEqual(client.data["stage"], "legacy")

    def test_watchdog_ignores_fresh_timestamp_from_old_generation(self):
        client = _GenerationRedis("job-old", "9999999999.0")

        updated_at = main._watchdog_progress_updated_at(
            client,
            "video:video-1:progress",
            "job-new",
        )

        self.assertIsNone(updated_at)

    def test_watchdog_accepts_only_current_generation_timestamp(self):
        client = _GenerationRedis("job-new", "123.5")

        updated_at = main._watchdog_progress_updated_at(
            client,
            "video:video-1:progress",
            "job-new",
        )

        self.assertEqual(updated_at, b"123.5")

    def test_pipeline_progress_calls_always_supply_job_generation(self):
        service_root = Path(__file__).resolve().parents[1]
        paths = sorted((service_root / "app" / "tasks").glob("*.py"))
        paths.append(service_root / "app" / "main.py")
        mutators = {
            "init_progress",
            "start_task",
            "update_task",
            "complete_task",
            "fail_task",
            "set_stage",
            "set_percent",
        }
        missing = []
        for path in paths:
            tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                function = node.func
                if not (
                    isinstance(function, ast.Attribute)
                    and isinstance(function.value, ast.Name)
                    and function.value.id == "progress_tracker"
                    and function.attr in mutators
                ):
                    continue
                if not any(
                    keyword.arg == "job_id"
                    for keyword in node.keywords
                ):
                    missing.append(
                        f"{path.name}:{node.lineno}:{function.attr}"
                    )

        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
