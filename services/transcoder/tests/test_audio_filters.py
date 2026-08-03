import unittest

from app.ffmpeg_utils import _audio_filter_chain


class AudioFilterChainTests(unittest.TestCase):
    def test_positive_audio_offset_uses_milliseconds(self):
        chain = _audio_filter_chain([], 105.0)

        self.assertEqual(
            chain,
            "asetpts=PTS-STARTPTS,adelay=105:all=1,aresample=async=1",
        )

    def test_negative_audio_offset_trims_instead_of_invalid_delay(self):
        chain = _audio_filter_chain([], -105.0)

        self.assertNotIn("adelay=-", chain)
        self.assertEqual(
            chain,
            "atrim=start=0.105000,asetpts=PTS-STARTPTS,aresample=async=1",
        )

    def test_zero_audio_offset_only_rebases_timestamps(self):
        chain = _audio_filter_chain(["volume=0.9"], 0.0)

        self.assertEqual(chain, "volume=0.9,asetpts=PTS-STARTPTS")


if __name__ == "__main__":
    unittest.main()
