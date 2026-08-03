import ast
import json
import math
from pathlib import Path
import unittest
from unittest.mock import patch

from app import main, progress


class _ProgressRedis:
    def __init__(
        self,
        job_id: str = "job-current",
        updated_at: str = "100.0",
        **values,
    ):
        self.data = {
            "job_id": job_id,
            "updated_at": updated_at,
            **{str(key): str(value) for key, value in values.items()},
        }

    def _generation_matches(self, requested_job) -> bool:
        requested = str(requested_job or "")
        current = str(self.data.get("job_id") or "")
        if current:
            return bool(requested) and current == requested
        return not requested

    def _task(self, name):
        raw = self.data.get(f"task:{name}")
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return {}

    def _store_task(self, name, payload):
        self.data[f"task:{name}"] = json.dumps(
            payload,
            separators=(",", ":"),
        )

    def _recompute(self, updated_at):
        total = int(self.data.get("total_tasks", "0"))
        completed = 0
        total_percent = 0
        for field in tuple(self.data):
            if not field.startswith("task:"):
                continue
            task = self._task(field[5:])
            task_percent = max(0, min(100, int(task.get("percent", 0))))
            if task.get("status") == "completed":
                completed += 1
                task_percent = 100
            total_percent += task_percent
        computed = math.floor(total_percent / total) if total else 0
        prior = int(float(self.data.get("percent", "0")))
        overall = max(prior, max(0, min(100, computed)))
        self.data["percent"] = str(overall)
        self.data["completed_tasks"] = str(completed)
        self.data["updated_at"] = str(updated_at)
        return overall

    def eval(self, script, _num_keys, _key, *args):
        if "HLS_RECOMPUTE_OVERALL_V1" in script:
            requested_job, updated_at = args
            if not self._generation_matches(requested_job):
                return 0
            return self._recompute(updated_at)

        if "HLS_SET_PERCENT_V1" in script:
            requested_job, requested, stage, updated_at = args
            if not self._generation_matches(requested_job):
                return 0
            prior = int(float(self.data.get("percent", "0")))
            requested = max(0, min(100, int(requested)))
            self.data["percent"] = str(max(prior, requested))
            if stage:
                self.data["stage"] = str(stage)
            self.data["updated_at"] = str(updated_at)
            return 1

        if "HLS_TASK_UPDATE_V2" in script:
            (
                requested_job,
                task_name,
                status,
                requested,
                stage,
                error,
                updated_at,
            ) = args
            if not self._generation_matches(requested_job):
                return 0
            if f"task:{task_name}" not in self.data:
                return -1
            task = self._task(task_name)
            if not task:
                return -1
            if (
                task.get("status") == "completed"
                and status != "completed"
            ):
                return 2
            requested = max(0, min(100, int(requested)))
            if status == "running":
                requested = min(99, requested)
            payload = {
                "status": str(status),
                "percent": max(
                    max(0, min(100, int(task.get("percent", 0)))),
                    requested,
                ),
            }
            if "chunks" in task:
                payload["chunks"] = task["chunks"]
            if status == "failed" and error:
                payload["error"] = str(error)
            self._store_task(task_name, payload)
            if stage:
                self.data["stage"] = str(stage)
            self.data["updated_at"] = str(updated_at)
            return 1

        if "HLS_REPLACE_TASK_V4" in script:
            (
                requested_job,
                old_name,
                pending,
                replacement_signature,
                *tail,
            ) = args
            if not self._generation_matches(requested_job):
                return 0
            names = tail[:-1]
            updated_at = tail[-1]
            old_field = f"task:{old_name}"
            replacement_field = f"_replacement:{old_name}"
            prior_replacement = self.data.get(replacement_field)
            if prior_replacement is not None:
                return (
                    2
                    if prior_replacement == replacement_signature
                    else -3
                )
            if old_field not in self.data or not self._task(old_name):
                return -1
            if self._task(old_name).get("status") == "completed":
                return -2
            keeps_old_name = old_name in names
            removed = 0
            if not keeps_old_name:
                removed = int(self.data.pop(old_field, None) is not None)
            added = 0
            for name in names:
                field = f"task:{name}"
                if field not in self.data:
                    added += 1
                    self.data[field] = str(pending)
            total = int(self.data.get("total_tasks", "0"))
            self.data["total_tasks"] = str(
                max(0, total - removed + added)
            )
            self.data[replacement_field] = str(replacement_signature)
            self.data["updated_at"] = str(updated_at)
            return 1

        if "HLS_CONFIGURE_CHUNKED_TASK_V1" in script:
            (
                requested_job,
                task_name,
                total_chunks,
                stage,
                updated_at,
            ) = args
            if not self._generation_matches(requested_job):
                return 0
            task = self._task(task_name)
            if not task:
                return -2
            total = int(total_chunks)
            if total < 1 or total > 10_000:
                return -3
            total_field = f"_chunk:total:{task_name}"
            existing_total = int(self.data.get(total_field, "0"))
            if existing_total and existing_total != total:
                return -1
            if task.get("status") == "completed":
                return 2
            base_field = f"_chunk:base:{task_name}"
            sum_field = f"_chunk:sum:{task_name}"
            completed_field = f"_chunk:completed:{task_name}"
            if not existing_total:
                base = max(0, min(99, int(task.get("percent", 0))))
                self.data[total_field] = str(total)
                self.data[base_field] = str(base)
                self.data[sum_field] = "0"
                self.data[completed_field] = "0"
            completed = int(self.data.get(completed_field, "0"))
            prior = max(0, min(99, int(task.get("percent", 0))))
            self._store_task(
                task_name,
                {
                    "status": "running",
                    "percent": prior,
                    "chunks": {
                        "completed": completed,
                        "total": total,
                    },
                },
            )
            if stage:
                self.data["stage"] = str(stage)
            self.data["updated_at"] = str(updated_at)
            return 1

        if "HLS_UPDATE_CHUNKED_TASK_V1" in script:
            (
                requested_job,
                task_name,
                chunk_index,
                total_chunks,
                requested,
                stage,
                updated_at,
            ) = args
            if not self._generation_matches(requested_job):
                return 0
            task = self._task(task_name)
            if not task:
                return -2
            total = int(total_chunks)
            if total < 1 or total > 10_000:
                return -3
            index = int(chunk_index)
            if index < 0 or index >= total:
                return -4
            configured = int(
                self.data.get(f"_chunk:total:{task_name}", "0")
            )
            if configured != total:
                return -1
            if task.get("status") == "completed":
                return 2
            value_field = f"_chunk:value:{task_name}:{index}"
            old_value = int(self.data.get(value_field, "0"))
            new_value = max(
                old_value,
                max(0, min(100, int(requested))),
            )
            sum_field = f"_chunk:sum:{task_name}"
            completed_field = f"_chunk:completed:{task_name}"
            progress_sum = min(
                total * 100,
                int(self.data.get(sum_field, "0"))
                + new_value
                - old_value,
            )
            completed = int(self.data.get(completed_field, "0"))
            if old_value < 100 <= new_value:
                completed = min(total, completed + 1)
            self.data[value_field] = str(new_value)
            self.data[sum_field] = str(progress_sum)
            self.data[completed_field] = str(completed)
            base = int(self.data.get(f"_chunk:base:{task_name}", "0"))
            aggregate = base + math.floor(
                ((100 - base) * progress_sum) / (total * 100)
            )
            aggregate = min(99, aggregate)
            aggregate = max(
                aggregate,
                max(0, min(99, int(task.get("percent", 0)))),
            )
            self._store_task(
                task_name,
                {
                    "status": "running",
                    "percent": aggregate,
                    "chunks": {
                        "completed": completed,
                        "total": total,
                    },
                },
            )
            if stage:
                self.data["stage"] = str(stage)
            self.data["updated_at"] = str(updated_at)
            return 1

        requested_job = args[0]
        if not self._generation_matches(requested_job):
            return 0
        for index in range(1, len(args), 2):
            self.data[str(args[index])] = str(args[index + 1])
        return 1

    def hget(self, _key, field):
        value = self.data.get(field)
        return value.encode() if value is not None else None

    def hgetall(self, _key):
        return {
            str(key).encode(): str(value).encode()
            for key, value in self.data.items()
        }


class ProgressGenerationTests(unittest.TestCase):
    def test_superseded_callback_cannot_refresh_current_updated_at(self):
        client = _ProgressRedis("job-new", "200.0")
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
        client = _ProgressRedis("job-new", "200.0")
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
        client = _ProgressRedis("", "100.0")
        with patch.object(progress, "_redis", return_value=client):
            progress.set_percent("video-legacy", 50, "legacy")

        self.assertEqual(client.data["percent"], "50")
        self.assertEqual(client.data["stage"], "legacy")

    def test_watchdog_ignores_fresh_timestamp_from_old_generation(self):
        client = _ProgressRedis("job-old", "9999999999.0")

        updated_at = main._watchdog_progress_updated_at(
            client,
            "video:video-1:progress",
            "job-new",
        )

        self.assertIsNone(updated_at)

    def test_watchdog_accepts_only_current_generation_timestamp(self):
        client = _ProgressRedis("job-new", "123.5")

        updated_at = main._watchdog_progress_updated_at(
            client,
            "video:video-1:progress",
            "job-new",
        )

        self.assertEqual(updated_at, b"123.5")

    def test_retried_task_replacement_does_not_decrement_total_twice(self):
        client = _ProgressRedis(
            total_tasks="1",
            **{"task:gpu_group": json.dumps(
                {"status": "running", "percent": 55}
            )},
        )
        with (
            patch.object(progress, "_redis", return_value=client),
            patch.object(progress, "_recompute_overall") as recompute,
        ):
            progress.replace_task(
                "video-1",
                "gpu_group",
                ["cpu_720p", "cpu_480p"],
                job_id="job-current",
            )
            progressed = json.dumps(
                {"status": "running", "percent": 74}
            )
            client.data["task:cpu_720p"] = progressed
            progress.replace_task(
                "video-1",
                "gpu_group",
                ["cpu_720p", "cpu_480p"],
                job_id="job-current",
            )

        self.assertEqual(client.data["total_tasks"], "2")
        self.assertNotIn("task:gpu_group", client.data)
        self.assertEqual(client.data["task:cpu_720p"], progressed)
        self.assertIn("task:cpu_480p", client.data)
        recompute.assert_called_once()

    def test_replacement_preserves_progress_when_old_name_is_reused(self):
        progressed = json.dumps({"status": "running", "percent": 81})
        client = _ProgressRedis(
            total_tasks="1",
            **{"task:transcode_720p": progressed},
        )
        with (
            patch.object(progress, "_redis", return_value=client),
            patch.object(progress, "_recompute_overall"),
        ):
            progress.replace_task(
                "video-1",
                "transcode_720p",
                ["transcode_720p", "transcode_480p"],
                job_id="job-current",
            )
            progress.replace_task(
                "video-1",
                "transcode_720p",
                ["transcode_720p", "transcode_480p"],
                job_id="job-current",
            )

        self.assertEqual(client.data["total_tasks"], "2")
        self.assertEqual(client.data["task:transcode_720p"], progressed)
        self.assertIn("task:transcode_480p", client.data)

    def test_missing_replacement_source_fails_closed(self):
        client = _ProgressRedis(total_tasks="1")
        original = dict(client.data)
        with (
            patch.object(progress, "_redis", return_value=client),
            patch.object(progress, "_recompute_overall") as recompute,
        ):
            replaced = progress.replace_task(
                "video-1",
                "missing",
                ["transcode_720p"],
                job_id="job-current",
            )

        self.assertFalse(replaced)
        self.assertEqual(client.data, original)
        recompute.assert_not_called()

    def test_completed_replacement_source_fails_closed(self):
        completed = json.dumps({"status": "completed", "percent": 100})
        client = _ProgressRedis(
            total_tasks="1",
            **{"task:direct": completed},
        )
        original = dict(client.data)
        with (
            patch.object(progress, "_redis", return_value=client),
            patch.object(progress, "_recompute_overall") as recompute,
        ):
            replaced = progress.replace_task(
                "video-1",
                "direct",
                ["transcode_720p"],
                job_id="job-current",
            )

        self.assertFalse(replaced)
        self.assertEqual(client.data, original)
        recompute.assert_not_called()

    def test_conflicting_replacement_replay_fails_closed(self):
        running = json.dumps({"status": "running", "percent": 25})
        client = _ProgressRedis(
            total_tasks="1",
            **{"task:direct": running},
        )
        with (
            patch.object(progress, "_redis", return_value=client),
            patch.object(progress, "_recompute_overall"),
        ):
            self.assertTrue(
                progress.replace_task(
                    "video-1",
                    "direct",
                    ["transcode_720p"],
                    job_id="job-current",
                )
            )
            after_first = dict(client.data)
            self.assertFalse(
                progress.replace_task(
                    "video-1",
                    "direct",
                    ["transcode_480p"],
                    job_id="job-current",
                )
            )

        self.assertEqual(client.data, after_first)
        self.assertIn("task:transcode_720p", client.data)
        self.assertNotIn("task:transcode_480p", client.data)

    def test_removed_task_update_cannot_resurrect_old_inventory(self):
        running = json.dumps({"status": "running", "percent": 25})
        client = _ProgressRedis(
            total_tasks="1",
            stage="Direct play",
            **{"task:direct": running},
        )
        with (
            patch.object(progress, "_redis", return_value=client),
            patch.object(progress, "_recompute_overall"),
        ):
            self.assertTrue(
                progress.replace_task(
                    "video-1",
                    "direct",
                    ["transcode_720p"],
                    job_id="job-current",
                )
            )
            replacement_updated_at = client.data["updated_at"]
            progress.start_task(
                "video-1",
                "direct",
                "Delayed direct delivery",
                job_id="job-current",
            )
            progress.update_task(
                "video-1",
                "direct",
                90,
                "Delayed direct delivery",
                job_id="job-current",
            )

        self.assertNotIn("task:direct", client.data)
        self.assertEqual(client.data["stage"], "Direct play")
        self.assertEqual(client.data["updated_at"], replacement_updated_at)

    def test_retried_start_and_lower_percent_never_regress(self):
        client = _ProgressRedis(
            percent="80",
            total_tasks="1",
            **{"task:transcode_720p": json.dumps(
                {"status": "running", "percent": 62}
            )},
        )
        with patch.object(progress, "_redis", return_value=client):
            progress.start_task(
                "video-1",
                "transcode_720p",
                "Retrying delivery",
                job_id="job-current",
            )
            progress.set_percent(
                "video-1",
                20,
                "Fallback active",
                job_id="job-current",
            )

        task = json.loads(client.data["task:transcode_720p"])
        self.assertEqual(task["percent"], 62)
        self.assertEqual(client.data["percent"], "80")
        self.assertEqual(client.data["stage"], "Fallback active")

    def test_chunk_progress_aggregates_out_of_order_and_hides_internals(self):
        client = _ProgressRedis(
            percent="60",
            total_tasks="2",
            completed_tasks="1",
            **{
                "task:audio_eng": json.dumps(
                    {"status": "completed", "percent": 100}
                ),
                "task:transcode_720p": json.dumps(
                    {"status": "running", "percent": 20}
                ),
            },
        )
        with patch.object(progress, "_redis", return_value=client):
            progress.configure_chunked_task(
                "video-1",
                "transcode_720p",
                3,
                job_id="job-current",
            )
            progress.update_chunked_task(
                "video-1",
                "transcode_720p",
                2,
                3,
                100,
                job_id="job-current",
            )
            after_first = progress.get_progress("video-1")
            progress.update_chunked_task(
                "video-1",
                "transcode_720p",
                2,
                3,
                10,
                job_id="job-current",
            )
            after_regression = progress.get_progress("video-1")
            progress.update_chunked_task(
                "video-1",
                "transcode_720p",
                0,
                3,
                80,
                job_id="job-current",
            )
            progress.update_chunked_task(
                "video-1",
                "transcode_720p",
                1,
                3,
                100,
                job_id="job-current",
            )
            progress.update_chunked_task(
                "video-1",
                "transcode_720p",
                0,
                3,
                100,
                job_id="job-current",
            )
            before_concat = progress.get_progress("video-1")
            progress.complete_task(
                "video-1",
                "transcode_720p",
                job_id="job-current",
            )
            completed = progress.get_progress("video-1")

        first_task = after_first["tasks"]["transcode_720p"]
        self.assertEqual(first_task["percent"], 46)
        self.assertEqual(first_task["chunks"], {"completed": 1, "total": 3})
        self.assertEqual(
            after_regression["tasks"]["transcode_720p"],
            first_task,
        )
        self.assertGreaterEqual(
            int(after_regression["percent"]),
            int(after_first["percent"]),
        )
        chunked_task = before_concat["tasks"]["transcode_720p"]
        self.assertEqual(chunked_task["percent"], 99)
        self.assertEqual(
            chunked_task["chunks"],
            {"completed": 3, "total": 3},
        )
        self.assertEqual(before_concat["percent"], 99)
        self.assertFalse(
            any(key.startswith("_chunk:") for key in before_concat)
        )
        self.assertEqual(
            completed["tasks"]["transcode_720p"]["status"],
            "completed",
        )
        self.assertEqual(completed["percent"], 100)
        self.assertEqual(completed["completed_tasks"], 2)

    def test_chunk_configuration_is_idempotent_but_rejects_new_total(self):
        client = _ProgressRedis(
            total_tasks="1",
            **{"task:transcode_720p": json.dumps(
                {"status": "running", "percent": 30}
            )},
        )
        with patch.object(progress, "_redis", return_value=client):
            self.assertTrue(progress.configure_chunked_task(
                "video-1",
                "transcode_720p",
                3,
                job_id="job-current",
            ))
            progress.update_chunked_task(
                "video-1",
                "transcode_720p",
                0,
                3,
                100,
                job_id="job-current",
            )
            self.assertTrue(progress.configure_chunked_task(
                "video-1",
                "transcode_720p",
                3,
                job_id="job-current",
            ))
            with self.assertRaisesRegex(ValueError, "result=-1"):
                progress.configure_chunked_task(
                    "video-1",
                    "transcode_720p",
                    4,
                    job_id="job-current",
                )

        task = json.loads(client.data["task:transcode_720p"])
        self.assertEqual(task["chunks"], {"completed": 1, "total": 3})
        self.assertGreater(task["percent"], 30)

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
            "replace_task",
            "configure_chunked_task",
            "update_chunked_task",
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
