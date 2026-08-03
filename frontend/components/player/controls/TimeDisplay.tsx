'use client';

import { usePlayer } from '../PlayerProvider';

export function TimeDisplay() {
  const { formattedCurrentTime, formattedDuration } = usePlayer();

  return (
    <div
      className="min-w-[5.5rem] text-sm tabular-nums text-white/90"
      aria-live="off"
    >
      {formattedCurrentTime} / {formattedDuration}
    </div>
  );
}
