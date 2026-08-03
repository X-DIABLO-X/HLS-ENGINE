'use client';

import { usePlayer } from './PlayerProvider';

interface VideoElementProps {
  poster?: string;
  title?: string;
}

export function VideoElement({ poster, title }: VideoElementProps) {
  const { videoRef, togglePlay } = usePlayer();

  return (
    <video
      ref={videoRef}
      poster={poster}
      playsInline
      controls={false}
      preload="metadata"
      crossOrigin="anonymous"
      className="h-full w-full object-contain"
      aria-label={title || 'Video player'}
      onClick={togglePlay}
    />
  );
}
