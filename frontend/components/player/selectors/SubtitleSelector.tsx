'use client';

import { usePlayer } from '../PlayerProvider';

export function SubtitleSelector() {
  const { subtitles, currentSubtitleTrack, setSubtitleTrack } = usePlayer();

  if (subtitles.length === 0) return null;

  return (
    <div className="flex flex-col gap-1">
      <span className="px-3 py-1 text-xs font-medium uppercase tracking-wide text-white/60">
        Subtitles
      </span>
      <button
        type="button"
        onClick={() => setSubtitleTrack(-1)}
        className={`px-3 py-2 text-left text-sm transition-colors hover:bg-white/10 ${
          currentSubtitleTrack === -1 ? 'text-accent' : 'text-white'
        }`}
        aria-pressed={currentSubtitleTrack === -1}
      >
        Off
      </button>
      {subtitles.map((track, index) => (
        <button
          key={track.id}
          type="button"
          onClick={() => setSubtitleTrack(index)}
          className={`px-3 py-2 text-left text-sm transition-colors hover:bg-white/10 ${
            currentSubtitleTrack === index ? 'text-accent' : 'text-white'
          }`}
          aria-pressed={currentSubtitleTrack === index}
        >
          {track.name}{' '}
          <span className="text-white/50">({track.lang.toUpperCase()})</span>
        </button>
      ))}
    </div>
  );
}
