import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app import models
from app.tasks.package import _rel_path, _write_master


class TrackIdentityTests(unittest.TestCase):
    def test_duplicate_languages_get_distinct_stable_identities_and_names(self):
        tracks = [
            {"language": "ind", "subtitle_index": 0},
            {"language": "ind", "subtitle_index": 1},
            {"language": "eng", "subtitle_index": 2},
        ]

        prepared = models.assign_track_identities(
            tracks, "subtitle_index"
        )

        self.assertEqual(
            [track["track_id"] for track in prepared],
            ["ind", "ind_2", "eng"],
        )
        self.assertEqual(
            [track["name"] for track in prepared],
            ["Indonesian", "Indonesian 2", "English"],
        )
        self.assertEqual(
            [track["language"] for track in prepared],
            ["ind", "ind", "eng"],
        )
        self.assertNotIn("track_id", tracks[0])

    def test_metadata_titles_are_not_exposed_as_track_labels(self):
        tracks = [
            {
                "language": "ind",
                "audio_index": 0,
                "tags": {"title": 'Main "mix"\n'},
            },
            {
                "language": "ind",
                "audio_index": 1,
                "tags": {"title": 'Main "mix"\n'},
            },
        ]

        prepared = models.assign_track_identities(tracks, "audio_index")

        self.assertEqual(prepared[0]["name"], "Indonesian")
        self.assertEqual(prepared[1]["name"], "Indonesian 2")

    def test_direct_task_fallback_is_path_safe_and_stream_specific(self):
        self.assertEqual(
            models.safe_track_identity(None, "ind", 0), "ind"
        )
        self.assertEqual(
            models.safe_track_identity(None, "ind", 1), "ind_stream_2"
        )
        self.assertEqual(
            models.safe_track_identity("../../escape", "ind", 1),
            "ind_stream_2",
        )
        self.assertEqual(
            models.safe_track_identity("ind_2", "ind", 1), "ind_2"
        )

    def test_invalid_container_language_is_never_used_as_a_path_or_attribute(self):
        prepared = models.assign_track_identities(
            [{"language": 'bad"\n../../x', "audio_index": 0}],
            "audio_index",
        )

        self.assertEqual(prepared[0]["language"], "und")
        self.assertEqual(prepared[0]["track_id"], "und")
        self.assertEqual(prepared[0]["name"], "Unknown language")

    def test_track_identity_can_be_recovered_for_package_fallback(self):
        track = SimpleNamespace(
            playlist_path="/work/output/audio_ind_2/audio.m3u8",
            file_path="/work/audio/ind_2/audio.m4a",
            language="ind",
        )
        raw_only = SimpleNamespace(
            playlist_path=None,
            file_path="/work/subtitles/ind_2/subtitles.vtt",
            language="ind",
        )

        self.assertEqual(
            models.track_identity_from_path(track, "audio"), "ind_2"
        )
        self.assertEqual(
            models.track_identity_from_path(raw_only, "subtitles"),
            "ind_2",
        )


class MasterPlaylistTrackTests(unittest.TestCase):
    def test_master_keeps_every_duplicate_language_track(self):
        with tempfile.TemporaryDirectory() as output_dir:
            def playlist(relative):
                path = os.path.join(output_dir, *relative.split("/"))
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write("#EXTM3U\n")
                return path

            audio = [
                SimpleNamespace(
                    language="hin",
                    name="Hindi",
                    default=True,
                    playlist_path=playlist("audio_hin/audio.m3u8"),
                ),
                SimpleNamespace(
                    language="ind",
                    name="Indonesian",
                    default=False,
                    playlist_path=playlist("audio_ind/audio.m3u8"),
                ),
                SimpleNamespace(
                    language="ind",
                    name="Commentary",
                    default=False,
                    playlist_path=playlist("audio_ind_2/audio.m3u8"),
                ),
            ]
            subtitles = [
                SimpleNamespace(
                    language="eng",
                    name="English",
                    playlist_path=playlist(
                        "subtitles_eng/subtitles.m3u8"
                    ),
                ),
                SimpleNamespace(
                    language="ind",
                    name="IND",
                    playlist_path=playlist(
                        "subtitles_ind/subtitles.m3u8"
                    ),
                ),
                SimpleNamespace(
                    language="ind",
                    name="IND 2",
                    playlist_path=playlist(
                        "subtitles_ind_2/subtitles.m3u8"
                    ),
                ),
            ]
            renditions = [
                SimpleNamespace(
                    bandwidth=2_000_000,
                    width=1280,
                    height=720,
                    codec="h264",
                    playlist_path=playlist("video_720p/video.m3u8"),
                )
            ]
            master_path = os.path.join(output_dir, "master.m3u8")

            with patch(
                "app.tasks.package.ffmpeg_utils.ffprobe",
                return_value={
                    "streams": [
                        {
                            "codec_type": "video",
                            "width": 1280,
                            "height": 720,
                        }
                    ]
                },
            ):
                _write_master(
                    master_path, output_dir, renditions, audio, subtitles
                )

            with open(master_path, encoding="utf-8") as handle:
                master = handle.read()

        self.assertEqual(master.count("TYPE=AUDIO"), 3)
        self.assertEqual(master.count("TYPE=SUBTITLES"), 3)
        self.assertEqual(master.count('LANGUAGE="ind"'), 4)
        self.assertIn('NAME="IND",', master)
        self.assertIn('NAME="IND 2",', master)
        self.assertIn('URI="subtitles_ind/subtitles.m3u8"', master)
        self.assertIn(
            'URI="subtitles_ind_2/subtitles.m3u8"', master
        )
        self.assertIn('CODECS="avc1.640028,mp4a.40.2"', master)
        self.assertEqual(master.count("DEFAULT=YES"), 1)

    def test_master_uniquifies_duplicate_persisted_names(self):
        with tempfile.TemporaryDirectory() as output_dir:
            paths = []
            for identity in ("ind", "ind_2"):
                directory = os.path.join(
                    output_dir, f"subtitles_{identity}"
                )
                os.makedirs(directory)
                path = os.path.join(directory, "subtitles.m3u8")
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write("#EXTM3U\n")
                paths.append(path)

            tracks = [
                SimpleNamespace(
                    language="ind", name="Signs", playlist_path=paths[0]
                ),
                SimpleNamespace(
                    language="ind", name="Signs", playlist_path=paths[1]
                ),
            ]
            master_path = os.path.join(output_dir, "master.m3u8")

            _write_master(master_path, output_dir, [], [], tracks)
            with open(master_path, encoding="utf-8") as handle:
                master = handle.read()

        self.assertIn('NAME="Signs",', master)
        self.assertIn('NAME="Signs 2",', master)

    def test_relative_asset_path_cannot_escape_output_tree(self):
        with tempfile.TemporaryDirectory() as output_dir:
            outside = os.path.join(
                os.path.dirname(output_dir), "outside.m3u8"
            )

            with self.assertRaises(ValueError):
                _rel_path(output_dir, outside)


if __name__ == "__main__":
    unittest.main()
