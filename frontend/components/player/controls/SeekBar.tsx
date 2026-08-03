'use client';

import { usePlayer } from '../PlayerProvider';

export function SeekBar() {
  const { currentTime, duration, progressPercent, bufferedPercent, seek } =
    usePlayer();

  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    seek(Number(event.target.value));
  };

  return (
    <div className="group relative flex h-5 flex-1 items-center">
      {/* Buffered progress */}
      <div
        className="pointer-events-none absolute left-0 top-1/2 h-1 -translate-y-1/2 rounded-full bg-white/30"
        style={{ width: `${bufferedPercent}%` }}
      />
      {/* Playback progress */}
      <div
        className="pointer-events-none absolute left-0 top-1/2 h-1 -translate-y-1/2 rounded-full bg-accent"
        style={{ width: `${progressPercent}%` }}
      />
      <input
        type="range"
        min={0}
        max={duration || 100}
        step={0.1}
        value={currentTime}
        onChange={handleChange}
        className="player-slider relative z-10"
        aria-label="Seek"
        aria-valuemin={0}
        aria-valuemax={duration || 100}
        aria-valuenow={currentTime}
        aria-valuetext={`${Math.round(currentTime)} seconds`}
      />
    </div>
  );
}
