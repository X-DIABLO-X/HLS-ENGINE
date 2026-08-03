'use client';

import { FastForward, Rewind } from 'lucide-react';
import { usePlayer } from '../PlayerProvider';

interface SkipButtonProps {
  direction: 'backward' | 'forward';
}

export function SkipButton({ direction }: SkipButtonProps) {
  const { seekRelative } = usePlayer();
  const isBackward = direction === 'backward';
  const label = isBackward ? 'Back 10 seconds' : 'Forward 10 seconds';

  return (
    <button
      type="button"
      onClick={() => seekRelative(isBackward ? -10 : 10)}
      className="relative grid h-9 w-9 place-items-center rounded-full text-white transition-colors hover:bg-white/10 focus-visible:ring-2 focus-visible:ring-white"
      aria-label={label}
      title={label}
    >
      {isBackward ? (
        <Rewind className="h-5 w-5" aria-hidden="true" />
      ) : (
        <FastForward className="h-5 w-5" aria-hidden="true" />
      )}
      <span className="pointer-events-none absolute top-1/2 -translate-y-[42%] text-[9px] font-bold leading-none">
        10
      </span>
    </button>
  );
}
