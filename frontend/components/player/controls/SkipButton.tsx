'use client';

import { Redo2, Undo2 } from 'lucide-react';
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
      className="relative flex h-10 w-11 items-center justify-center rounded-md text-white transition-colors hover:bg-white/10 focus-visible:ring-2 focus-visible:ring-white"
      aria-label={label}
      title={label}
    >
      {isBackward ? (
        <Undo2 aria-hidden="true" className="h-6 w-6 -translate-y-0.5" strokeWidth={2.35} />
      ) : (
        <Redo2 aria-hidden="true" className="h-6 w-6 -translate-y-0.5" strokeWidth={2.35} />
      )}
      <span className="pointer-events-none absolute bottom-0.5 text-[10px] font-bold leading-none tabular-nums">
        10
      </span>
    </button>
  );
}
