import unittest

from app.tasks.extract_audio import AUDIO_MAX_RETRIES, _audio_retry_stage, extract_audio


class AudioRetryTests(unittest.TestCase):
    def test_audio_task_has_bounded_retries(self):
        self.assertEqual(extract_audio.max_retries, AUDIO_MAX_RETRIES)
        self.assertEqual(AUDIO_MAX_RETRIES, 5)

    def test_retry_stage_reports_next_retry(self):
        self.assertEqual(
            _audio_retry_stage("hin", completed_retries=0, max_retries=5),
            "Retrying audio (hin) 1/5",
        )
        self.assertEqual(
            _audio_retry_stage("ind", completed_retries=4, max_retries=5),
            "Retrying audio (ind) 5/5",
        )


if __name__ == "__main__":
    unittest.main()
