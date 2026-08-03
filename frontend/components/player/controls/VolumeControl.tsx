'use client';

import { Volume2, VolumeX, Volume1 } from 'lucide-react';
import { usePlayer } from '../PlayerProvider';

export function VolumeControl() {
  const { volume, isMuted, setVolume, toggleMute } = usePlayer();

  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setVolume(Number(event.target.value));
  };

  const displayedVolume = isMuted ? 0 : volume;

  return (
    <div className="flex items-center gap-2">
      <button
        type="button"
        onClick={toggleMute}
        className="rounded-full p-2 text-white transition-colors hover:bg-white/10"
        aria-label={isMuted ? 'Unmute' : 'Mute'}
        title={isMuted ? 'Unmute (m)' : 'Mute (m)'}
      >
        {displayedVolume === 0 ? (
          <VolumeX className="h-5 w-5" />
        ) : displayedVolume < 0.5 ? (
          <Volume1 className="h-5 w-5" />
        ) : (
          <Volume2 className="h-5 w-5" />
        )}
      </button>
      <input
        type="range"
        min={0}
        max={1}
        step={0.05}
        value={displayedVolume}
        onChange={handleChange}
        className="player-slider w-20 md:w-28"
        aria-label="Volume"
      />
    </div>
  );
}
