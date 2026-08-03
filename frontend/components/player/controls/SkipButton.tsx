'use client';

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
      className="flex h-9 min-w-11 items-center justify-center rounded-md px-1.5 text-xs font-semibold tabular-nums text-white transition-colors hover:bg-white/10 focus-visible:ring-2 focus-visible:ring-white"
      aria-label={label}
      title={label}
    >
      {isBackward ? '−10' : '+10'}
      <span className="ml-0.5 text-[10px] font-medium text-white/60">s</span>
    </button>
  );
}
