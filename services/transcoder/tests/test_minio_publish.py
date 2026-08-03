import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from minio.error import S3Error

from app.minio_client import atomic_publish, published_prefix_exists


class _Settings:
    MINIO_BUCKET = "hls-output"


class _FakeMinio:
    def __init__(self):
        self.calls = []
        self.objects = set()
        self.fail_once_on = None
        self.fail_after_store_on = None
        self._failed = set()
        self._lock = threading.Lock()
        self._active_uploads = 0
        self.max_active_uploads = 0
        self.upload_delay = 0

    def fput_object(self, bucket, object_name, local_path):
        with self._lock:
            self.calls.append(("put", object_name))
            self._active_uploads += 1
            self.max_active_uploads = max(
                self.max_active_uploads,
                self._active_uploads,
            )
        try:
            if self.upload_delay:
                time.sleep(self.upload_delay)
            with self._lock:
                if (
                    object_name == self.fail_once_on
                    and object_name not in self._failed
                ):
                    self._failed.add(object_name)
                    raise OSError("injected upload failure")

                self.objects.add(object_name)
                if (
                    object_name == self.fail_after_store_on
                    and object_name not in self._failed
                ):
                    self._failed.add(object_name)
                    raise OSError("injected lost response")
        finally:
            with self._lock:
                self._active_uploads -= 1

    def remove_object(self, bucket, object_name):
        with self._lock:
            self.calls.append(("remove", object_name))
            self.objects.discard(object_name)

    def list_objects(self, bucket, prefix, recursive=True):
        with self._lock:
            names = sorted(
                object_name
                for object_name in self.objects
                if object_name.startswith(prefix)
            )
        return [
            SimpleNamespace(object_name=object_name)
            for object_name in names
        ]


def _write_tree(root: Path, entries):
    if not isinstance(entries, dict):
        entries = {relative_path: relative_path for relative_path in entries}
    for relative_path, content in entries.items():
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def _valid_video_tree(
    segment_names=("segment_0001.m4s",),
    playlist_dir="video",
):
    playlist_name = f"{playlist_dir}/video.m3u8"
    entries = {
        "master.m3u8": (
            "#EXTM3U\n"
            "#EXT-X-STREAM-INF:BANDWIDTH=1000000\n"
            f"{playlist_name}\n"
        ),
        playlist_name: "#EXTM3U\n#EXT-X-PLAYLIST-TYPE:VOD\n",
    }
    for segment_name in segment_names:
        entries[playlist_name] += f"#EXTINF:6.000,\n{segment_name}\n"
        entries[f"{playlist_dir}/{segment_name}"] = "media"
    entries[playlist_name] += "#EXT-X-ENDLIST\n"
    return entries


class AtomicPublishTests(unittest.TestCase):
    def _publish(self, client, local_dir, prefix="ab/video/v1/"):
        with (
            patch("app.minio_client.get_client", return_value=client),
            patch("app.minio_client.get_settings", return_value=_Settings()),
        ):
            return atomic_publish(str(local_dir), prefix)

    def test_assets_then_media_playlists_then_master(self):
        client = _FakeMinio()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_tree(
                root,
                {
                    "master.m3u8": (
                        "#EXTM3U\n"
                        '#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",'
                        'NAME="Main",URI="audio/audio.m3u8"\n'
                        "#EXT-X-STREAM-INF:BANDWIDTH=1000000,"
                        'AUDIO="audio"\n'
                        "video/video.m3u8\n"
                    ),
                    "video/segment_0001.m4s": "video one",
                    "video/segment_0002.m4s": "video two",
                    "video/video.m3u8": (
                        "#EXTM3U\n"
                        "#EXTINF:6.000,\nsegment_0001.m4s\n"
                        "#EXTINF:6.000,\nsegment_0002.m4s\n"
                        "#EXT-X-ENDLIST\n"
                    ),
                    "audio/audio_0001.aac": "audio one",
                    "audio/audio.m3u8": (
                        "#EXTM3U\n"
                        "#EXTINF:6.000,\naudio_0001.aac\n"
                        "#EXT-X-ENDLIST\n"
                    ),
                },
            )

            result = self._publish(client, root)

        self.assertEqual(result, "ab/video/v1/")
        put_names = [name for operation, name in client.calls if operation == "put"]
        master_name = "ab/video/v1/master.m3u8"
        playlist_names = {
            "ab/video/v1/video/video.m3u8",
            "ab/video/v1/audio/audio.m3u8",
        }
        asset_names = set(put_names) - playlist_names - {master_name}

        self.assertEqual(put_names[-1], master_name)
        self.assertLess(
            max(put_names.index(name) for name in asset_names),
            min(put_names.index(name) for name in playlist_names),
        )
        self.assertLess(
            max(put_names.index(name) for name in playlist_names),
            put_names.index(master_name),
        )
        self.assertEqual(client.calls[0], ("remove", master_name))
        self.assertNotIn(".tmp", " ".join(put_names))

    def test_failed_asset_upload_leaves_master_absent(self):
        client = _FakeMinio()
        master_name = "ab/video/v1/master.m3u8"
        client.objects.add(master_name)
        client.fail_once_on = "ab/video/v1/video/segment.m4s"

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_tree(
                root,
                _valid_video_tree(("segment.m4s",)),
            )

            with self.assertRaises(OSError):
                self._publish(client, root)

        self.assertNotIn(master_name, client.objects)
        put_names = [name for operation, name in client.calls if operation == "put"]
        self.assertNotIn("ab/video/v1/video/video.m3u8", put_names)
        self.assertNotIn(master_name, put_names)

    def test_retry_is_idempotent_and_commits_only_successful_attempt(self):
        client = _FakeMinio()
        master_name = "ab/video/v1/master.m3u8"
        client.fail_once_on = "ab/video/v1/video/video.m3u8"

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_tree(
                root,
                _valid_video_tree(("segment.m4s",)),
            )

            with self.assertRaises(OSError):
                self._publish(client, root)
            self.assertNotIn(master_name, client.objects)

            self.assertEqual(self._publish(client, root), "ab/video/v1/")

        self.assertIn(master_name, client.objects)
        put_names = [name for operation, name in client.calls if operation == "put"]
        self.assertEqual(put_names.count(master_name), 1)
        self.assertEqual(put_names[-1], master_name)
        self.assertEqual(
            client.objects,
            {
                master_name,
                "ab/video/v1/video/segment.m4s",
                "ab/video/v1/video/video.m3u8",
            },
        )

    def test_retry_removes_stale_remote_segments_before_master_commit(self):
        client = _FakeMinio()
        stale = "ab/video/v1/video/segment_9999.m4s"
        outside = "ab/video/v10/video/keep.m4s"
        client.objects.update(
            {
                "ab/video/v1/master.m3u8",
                stale,
                outside,
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_tree(root, _valid_video_tree(("segment_0001.m4s",)))
            self.assertEqual(self._publish(client, root), "ab/video/v1/")

        self.assertNotIn(stale, client.objects)
        self.assertIn(outside, client.objects)
        self.assertIn("ab/video/v1/master.m3u8", client.objects)
        self.assertLess(
            client.calls.index(("remove", stale)),
            client.calls.index(("put", "ab/video/v1/master.m3u8")),
        )

    def test_lost_master_put_response_is_rolled_back(self):
        client = _FakeMinio()
        master_name = "ab/video/v1/master.m3u8"
        client.fail_after_store_on = master_name

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_tree(root, _valid_video_tree(("segment.m4s",)))

            with self.assertRaises(OSError):
                self._publish(client, root)

        self.assertNotIn(master_name, client.objects)
        self.assertEqual(client.calls[-1], ("remove", master_name))

    def test_invalid_local_presentation_withholds_existing_master(self):
        client = _FakeMinio()
        master_name = "ab/video/v1/master.m3u8"
        client.objects.add(master_name)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_tree(root, ["video/segment.m4s"])

            with self.assertRaises(ValueError):
                self._publish(client, root)

        self.assertNotIn(master_name, client.objects)
        self.assertEqual(client.calls[0], ("remove", master_name))
        self.assertEqual(client.calls[-1], ("remove", master_name))

    def test_object_paths_are_portable_and_prefix_is_validated(self):
        client = _FakeMinio()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_tree(
                root,
                _valid_video_tree(
                    ("segment.m4s",),
                    playlist_dir="nested/deep",
                ),
            )

            original_prefix = r"library\movie\v2\\"
            self.assertEqual(
                self._publish(client, root, original_prefix),
                original_prefix,
            )

            with self.assertRaises(ValueError):
                self._publish(client, root, "library/../private/")

        put_names = [name for operation, name in client.calls if operation == "put"]
        self.assertIn("library/movie/v2/nested/deep/segment.m4s", put_names)
        self.assertNotIn("\\", " ".join(put_names))

    def test_master_with_zero_renditions_is_rejected_before_upload(self):
        client = _FakeMinio()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_tree(root, {"master.m3u8": "#EXTM3U\n"})

            with self.assertRaisesRegex(ValueError, "at least one"):
                self._publish(client, root)

        self.assertFalse(
            any(operation == "put" for operation, _ in client.calls)
        )

    def test_missing_master_playlist_reference_is_rejected(self):
        client = _FakeMinio()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_tree(
                root,
                {
                    "master.m3u8": (
                        "#EXTM3U\n"
                        "#EXT-X-STREAM-INF:BANDWIDTH=1000000\n"
                        "video/missing.m3u8\n"
                    )
                },
            )

            with self.assertRaisesRegex(ValueError, "reference is missing"):
                self._publish(client, root)

        self.assertFalse(
            any(operation == "put" for operation, _ in client.calls)
        )

    def test_incomplete_or_empty_media_playlist_is_rejected(self):
        for invalid_playlist, expected in (
            (
                "#EXTM3U\n#EXTINF:6.000,\nsegment.m4s\n",
                "ENDLIST missing",
            ),
            (
                "#EXTM3U\n#EXT-X-ENDLIST\n",
                "no media URI",
            ),
        ):
            with self.subTest(expected=expected):
                client = _FakeMinio()
                with tempfile.TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    entries = _valid_video_tree(("segment.m4s",))
                    entries["video/video.m3u8"] = invalid_playlist
                    _write_tree(root, entries)

                    with self.assertRaisesRegex(ValueError, expected):
                        self._publish(client, root)

                self.assertFalse(
                    any(
                        operation == "put"
                        for operation, _ in client.calls
                    )
                )

    def test_media_playlist_with_missing_asset_is_rejected(self):
        client = _FakeMinio()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            entries = _valid_video_tree(("segment.m4s",))
            del entries["video/segment.m4s"]
            _write_tree(root, entries)

            with self.assertRaisesRegex(ValueError, "reference is missing"):
                self._publish(client, root)

        self.assertFalse(
            any(operation == "put" for operation, _ in client.calls)
        )

    def test_valid_single_object_subtitle_playlist_is_accepted(self):
        client = _FakeMinio()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            entries = _valid_video_tree(("segment.m4s",))
            entries["master.m3u8"] = (
                "#EXTM3U\n"
                '#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",'
                'NAME="English",URI="subtitles/subtitles.m3u8"\n'
                "#EXT-X-STREAM-INF:BANDWIDTH=1000000,"
                'SUBTITLES="subs"\n'
                "video/video.m3u8\n"
            )
            entries["subtitles/subtitles.m3u8"] = (
                "#EXTM3U\n"
                "#EXT-X-PLAYLIST-TYPE:VOD\n"
                "#EXTINF:7177.832,\n"
                "subtitles.vtt\n"
                "#EXT-X-ENDLIST\n"
            )
            entries["subtitles/subtitles.vtt"] = "WEBVTT\n"
            _write_tree(root, entries)

            self.assertEqual(self._publish(client, root), "ab/video/v1/")

        self.assertIn(
            "ab/video/v1/subtitles/subtitles.m3u8",
            client.objects,
        )

    def test_upload_concurrency_is_bounded_by_environment(self):
        client = _FakeMinio()
        client.upload_delay = 0.03
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_tree(
                root,
                _valid_video_tree(
                    tuple(
                        f"segment_{index:02d}.m4s"
                        for index in range(8)
                    )
                ),
            )

            with patch.dict(os.environ, {"HLS_PUBLISH_MAX_WORKERS": "3"}):
                self._publish(client, root)

        self.assertGreater(client.max_active_uploads, 1)
        self.assertLessEqual(client.max_active_uploads, 3)


class PublishedPrefixTests(unittest.TestCase):
    def _exists(self, stat_result):
        client = MagicMock()
        client.stat_object.return_value = stat_result
        with (
            patch("app.minio_client.get_client", return_value=client),
            patch("app.minio_client.get_settings", return_value=_Settings()),
        ):
            return published_prefix_exists("ab/video/v1/")

    def test_only_non_empty_master_is_a_commit_marker(self):
        self.assertTrue(self._exists(SimpleNamespace(size=128)))
        self.assertFalse(self._exists(SimpleNamespace(size=0)))
        self.assertFalse(self._exists(SimpleNamespace()))

    def test_unexpected_stat_failure_fails_closed(self):
        client = MagicMock()
        client.stat_object.side_effect = OSError("storage unavailable")
        with (
            patch("app.minio_client.get_client", return_value=client),
            patch("app.minio_client.get_settings", return_value=_Settings()),
        ):
            with self.assertRaises(OSError):
                published_prefix_exists("ab/video/v1/")

    def test_missing_master_is_not_a_published_prefix(self):
        client = MagicMock()
        client.stat_object.side_effect = S3Error(
            None,
            "NoSuchKey",
            "missing",
            "/hls-output/ab/video/v1/master.m3u8",
            "request-id",
            "host-id",
        )
        with (
            patch("app.minio_client.get_client", return_value=client),
            patch("app.minio_client.get_settings", return_value=_Settings()),
        ):
            self.assertFalse(published_prefix_exists("ab/video/v1/"))


if __name__ == "__main__":
    unittest.main()
