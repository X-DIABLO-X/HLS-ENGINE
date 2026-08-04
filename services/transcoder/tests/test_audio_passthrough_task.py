import tempfile
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

from app import models
from app.tasks import extract_audio as audio_task


class _Query:
    def __init__(self, session, model):
        self.session = session
        self.model = model

    def filter(self, *_criteria):
        return self

    def order_by(self, *_criteria):
        return self

    def first(self):
        if self.model is models.Video:
            return self.session.video
        return None


class _Session:
    def __init__(self, video):
        self.video = video
        self.added = []
        self.commit_count = 0
        self.closed = False

    def query(self, model):
        return _Query(self, model)

    def add(self, row):
        self.added.append(row)

    def commit(self):
        self.commit_count += 1

    def close(self):
        self.closed = True


class AudioPassthroughTaskTests(unittest.TestCase):
    def _run_task(
        self,
        *,
        fail_copy=False,
        invalid_copy_output=False,
        source_channels=2,
        configured_channels=2,
        snapshot_passthrough=True,
    ):
        job_id = "job-audio-copy"
        video_id = "video-audio-copy"
        job = SimpleNamespace(id=job_id, video_id=video_id)
        video = SimpleNamespace(id=video_id, duration=12.0)
        db = _Session(video)
        progress_commands = []
        package_commands = []
        validation_calls = []

        def run_with_progress(cmd, _duration, _on_progress):
            progress_commands.append(cmd)
            if fail_copy and len(progress_commands) == 1:
                raise RuntimeError("simulated copy failure")

        def validate_output(*_args):
            validation_calls.append(True)
            if invalid_copy_output and len(validation_calls) == 1:
                raise ValueError("simulated invalid copied HLS")

        with tempfile.TemporaryDirectory() as work_dir, ExitStack() as stack:
            stack.enter_context(
                patch.object(audio_task, "SessionLocal", return_value=db)
            )
            stack.enter_context(
                patch.object(
                    audio_task,
                    "get_settings",
                    return_value=SimpleNamespace(
                        WORK_DIR=work_dir,
                        AAC_PASSTHROUGH_ENABLED=True,
                    ),
                )
            )
            stack.enter_context(
                patch.object(
                    audio_task,
                    "lock_current_job",
                    return_value=(job, video),
                )
            )
            stack.enter_context(
                patch.object(audio_task, "advance_job_status")
            )
            stack.enter_context(
                patch.object(
                    audio_task,
                    "ensure_local_source",
                    return_value="source.mkv",
                )
            )
            stack.enter_context(
                patch.object(audio_task, "progress_tracker")
            )
            stack.enter_context(
                patch.object(audio_task, "publish_event")
            )
            stack.enter_context(
                patch.object(
                    audio_task,
                    "_validate_audio_output",
                    side_effect=validate_output,
                )
            )
            stack.enter_context(
                patch.object(
                    audio_task.ffmpeg_utils,
                    "run_cmd_with_progress",
                    side_effect=run_with_progress,
                )
            )
            stack.enter_context(
                patch.object(
                    audio_task.ffmpeg_utils,
                    "run_cmd",
                    side_effect=lambda cmd: package_commands.append(cmd),
                )
            )

            result = audio_task._run_extract_audio(
                SimpleNamespace(
                    request=SimpleNamespace(retries=0),
                    max_retries=5,
                ),
                job_id,
                "unused-source-url",
                {
                    "audio_index": 0,
                    "track_id": "eng-0",
                    "language": "eng",
                    "name": "English",
                    "codec_name": "aac",
                    "profile": "LC",
                    "channels": source_channels,
                    "sample_rate": "48000",
                    "delay_ms": 0.0,
                    "bit_rate": "192000",
                },
                {
                    "audio_bitrate_kbps": 128,
                    "audio_channels": configured_channels,
                    "segment_duration_sec": 6,
                    "loudnorm": False,
                    "aac_passthrough_enabled": snapshot_passthrough,
                },
            )

        self.assertEqual(result["type"], "audio")
        self.assertTrue(db.closed)
        return progress_commands, package_commands, validation_calls, db.added

    def test_real_task_selects_copy_for_eligible_aac(self):
        commands, package_commands, validation_calls, tracks = self._run_task()

        self.assertEqual(len(commands), 1)
        self.assertEqual(
            commands[0][commands[0].index("-c:a") + 1],
            "copy",
        )
        self.assertEqual(package_commands, [])
        self.assertEqual(len(validation_calls), 1)
        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0].bitrate, 192000)

    def test_disabled_snapshot_ignores_enabled_worker_environment(self):
        commands, package_commands, _validation_calls, _tracks = (
            self._run_task(snapshot_passthrough=False)
        )

        self.assertEqual(
            commands[0][commands[0].index("-c:a") + 1],
            "aac",
        )
        self.assertEqual(package_commands, [])

    def test_copy_failure_falls_back_to_existing_two_step_encode(self):
        commands, package_commands, validation_calls, tracks = self._run_task(
            fail_copy=True,
        )

        self.assertEqual(len(commands), 2)
        self.assertEqual(
            commands[0][commands[0].index("-c:a") + 1],
            "copy",
        )
        self.assertEqual(
            commands[1][commands[1].index("-c:a") + 1],
            "aac",
        )
        self.assertEqual(
            commands[1][commands[1].index("-ac") + 1],
            str(tracks[0].channels),
        )
        self.assertEqual(len(package_commands), 1)
        self.assertEqual(len(validation_calls), 1)
        self.assertEqual(tracks[0].bitrate, 128000)

    def test_configured_stereo_forces_downmix_instead_of_copying_5_1(self):
        commands, package_commands, validation_calls, tracks = self._run_task(
            source_channels=6,
            configured_channels=2,
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(
            commands[0][commands[0].index("-c:a") + 1],
            "aac",
        )
        self.assertEqual(
            commands[0][commands[0].index("-ac") + 1],
            "2",
        )
        self.assertEqual(package_commands, [])
        self.assertEqual(len(validation_calls), 1)
        self.assertEqual(tracks[0].channels, 2)
        self.assertEqual(tracks[0].bitrate, 128000)

    def test_invalid_copied_hls_falls_back_to_two_step_encode(self):
        commands, package_commands, validation_calls, tracks = self._run_task(
            invalid_copy_output=True,
        )

        self.assertEqual(len(commands), 2)
        self.assertEqual(
            commands[0][commands[0].index("-c:a") + 1],
            "copy",
        )
        self.assertEqual(
            commands[1][commands[1].index("-c:a") + 1],
            "aac",
        )
        self.assertEqual(len(package_commands), 1)
        self.assertEqual(len(validation_calls), 2)
        self.assertEqual(tracks[0].bitrate, 128000)


if __name__ == "__main__":
    unittest.main()
