import unittest

from app.ingestion import ingestion_job_id, minio_source_url


class IngestionIdentityTests(unittest.TestCase):
    def test_same_object_always_has_same_job_id(self):
        first = ingestion_job_id(
            "80bc2f96-3c6d-42e8-b34f-1ea8613668f2",
            "uploads-raw",
            "raw/video/source.mp4",
        )
        second = ingestion_job_id(
            "80bc2f96-3c6d-42e8-b34f-1ea8613668f2",
            "uploads-raw",
            "raw/video/source.mp4",
        )

        self.assertEqual(first, second)

    def test_object_identity_changes_job_id(self):
        first = ingestion_job_id("video-1", "uploads-raw", "raw/video/source-a.mp4")
        second = ingestion_job_id("video-1", "uploads-raw", "raw/video/source-b.mp4")

        self.assertNotEqual(first, second)

    def test_minio_source_url_is_canonical(self):
        self.assertEqual(
            minio_source_url("uploads-raw/", "/raw/video/source.mp4"),
            "minio://uploads-raw/raw/video/source.mp4",
        )


if __name__ == "__main__":
    unittest.main()
