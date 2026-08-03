'use client';

import { Play, Pause } from 'lucide-react';
import { usePlayer } from '../PlayerProvider';

export function PlayPauseButton() {
  const { isPlaying, togglePlay } = usePlayer();

  return (
    <button
      type="button"
      onClick={togglePlay}
      className="rounded-full p-2 text-white transition-colors hover:bg-white/10 focus-visible:ring-2 focus-visible:ring-white"
      aria-label={isPlaying ? 'Pause' : 'Play'}
      title={isPlaying ? 'Pause (k)' : 'Play (k)'}
    >
      {isPlaying ? (
        <Pause className="h-6 w-6 fill-current" />
      ) : (
        <Play className="h-6 w-6 fill-current" />
      )}
    </button>
  );
}
