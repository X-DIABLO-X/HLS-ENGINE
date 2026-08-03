'use client';

import { useState, useCallback } from 'react';
import { PlayerProvider, usePlayer } from './PlayerProvider';
import { VideoElement } from './VideoElement';
import { ControlBar } from './controls/ControlBar';
import { AlertCircle, Loader2 } from 'lucide-react';

function PlayerInner({ title, poster }: { title?: string; poster?: string }) {
  const { containerRef, isLoading, error } = usePlayer();
  const [showControls, setShowControls] = useState(true);
  const [hideTimeout, setHideTimeout] =
    useState<ReturnType<typeof setTimeout> | null>(null);

  const scheduleHide = useCallback(() => {
    if (hideTimeout) clearTimeout(hideTimeout);
    setHideTimeout(
      setTimeout(() => {
        setShowControls(false);
      }, 3000)
    );
  }, [hideTimeout]);

  const handleMouseMove = useCallback(() => {
    setShowControls(true);
    scheduleHide();
  }, [scheduleHide]);

  const handleMouseLeave = useCallback(() => {
    setShowControls(false);
  }, []);

  const displayLoading = isLoading;

  return (
    <div
      ref={containerRef}
      className={`player-container group relative aspect-video w-full overflow-hidden rounded-xl bg-black focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent ${
        !showControls ? 'user-inactive' : ''
      }`}
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
      tabIndex={0}
      role="region"
      aria-label={`Video player${title ? `: ${title}` : ''}`}
    >
      <VideoElement poster={poster} title={title} />

      {displayLoading && (
        <div className="pointer-events-none absolute inset-0 z-20 flex flex-col items-center justify-center gap-3 bg-black/40">
          <Loader2 className="h-10 w-10 animate-spin text-white" />
          <span className="text-sm font-medium text-white/90">
            Loading stream…
          </span>
        </div>
      )}

      {error && (
        <div className="absolute inset-0 z-30 flex flex-col items-center justify-center gap-3 bg-black/80 p-6 text-center">
          <AlertCircle className="h-10 w-10 text-red-400" />
          <p className="max-w-md text-sm text-white/90">{error.message}</p>
          <p className="text-xs text-white/60">
            Please check your connection or try again later.
          </p>
        </div>
      )}

      <div className="absolute inset-x-0 bottom-0 z-20">
        <ControlBar />
      </div>
    </div>
  );
}

interface PlayerProps {
  manifestUrl: string;
  title?: string;
  poster?: string;
  onError?: (error: Error) => void;
}

export function Player({ manifestUrl, title, poster, onError }: PlayerProps) {
  const [instanceKey, setInstanceKey] = useState(0);

  const handleError = useCallback(
    (error: Error) => {
      onError?.(error);
    },
    [onError]
  );

  return (
    <PlayerProvider
      key={`${manifestUrl}-${instanceKey}`}
      manifestUrl={manifestUrl}
      onError={handleError}
    >
      <PlayerInner title={title} poster={poster} />
    </PlayerProvider>
  );
}
