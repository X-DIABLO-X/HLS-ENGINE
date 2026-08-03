'use client';

import { usePlayer } from '../PlayerProvider';

export function AudioTrackSelector() {
  const { audioTracks, currentAudioTrack, setAudioTrack } = usePlayer();

  if (audioTracks.length <= 1) return null;

  return (
    <div className="flex flex-col gap-1 p-1">
      {audioTracks.map((track, index) => (
        <button
          key={track.id}
          type="button"
          onClick={() => setAudioTrack(index)}
          className={`rounded-md px-3 py-2.5 text-left text-sm transition-colors hover:bg-white/10 ${
            currentAudioTrack === index ? 'bg-accent/10 text-accent' : 'text-white'
          }`}
          aria-pressed={currentAudioTrack === index}
        >
          {track.name}{' '}
          <span className="text-white/50">({track.lang.toUpperCase()})</span>
        </button>
      ))}
    </div>
  );
}
