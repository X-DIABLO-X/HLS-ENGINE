'use client';

import { Maximize, Minimize } from 'lucide-react';
import { usePlayer } from '../PlayerProvider';

export function FullscreenButton() {
  const { isFullscreen, toggleFullscreen } = usePlayer();

  return (
    <button
      type="button"
      onClick={toggleFullscreen}
      className="rounded-full p-2 text-white transition-colors hover:bg-white/10"
      aria-label={isFullscreen ? 'Exit fullscreen' : 'Enter fullscreen'}
      title={isFullscreen ? 'Exit fullscreen (f)' : 'Enter fullscreen (f)'}
    >
      {isFullscreen ? (
        <Minimize className="h-5 w-5" />
      ) : (
        <Maximize className="h-5 w-5" />
      )}
    </button>
  );
}
