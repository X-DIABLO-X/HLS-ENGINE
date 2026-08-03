'use client';

import {
  createContext,
  useContext,
  useMemo,
  useRef,
  useState,
  useCallback,
  useEffect,
  RefObject,
} from 'react';
import { useHls, UseHlsReturn } from './hooks/useHls';
import { useKeyboardShortcuts } from './hooks/useKeyboardShortcuts';
import { formatDuration } from '@/lib/api';

interface PlayerContextValue extends UseHlsReturn {
  videoRef: RefObject<HTMLVideoElement | null>;
  containerRef: RefObject<HTMLDivElement | null>;
  isPlaying: boolean;
  isMuted: boolean;
  isFullscreen: boolean;
  volume: number;
  currentTime: number;
  duration: number;
  buffered: number;
  togglePlay: () => void;
  play: () => void;
  pause: () => void;
  seek: (time: number) => void;
  seekRelative: (delta: number) => void;
  setVolume: (volume: number) => void;
  toggleMute: () => void;
  changeVolumeRelative: (delta: number) => void;
  toggleFullscreen: () => void;
  formattedCurrentTime: string;
  formattedDuration: string;
  progressPercent: number;
  bufferedPercent: number;
}

const PlayerContext = createContext<PlayerContextValue | null>(null);

interface PlayerProviderProps {
  children: React.ReactNode;
  manifestUrl: string;
  onError?: (error: Error) => void;
}

export function PlayerProvider({
  children,
  manifestUrl,
  onError,
}: PlayerProviderProps) {
  const videoRef = useRef<HTMLVideoElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const [isPlaying, setIsPlaying] = useState(false);
  const [isMuted, setIsMuted] = useState(false);
  const [volume, setVolumeState] = useState(1);
  const [currentTime, setCurrentTime] = useState(0);
  const [duration, setDuration] = useState(0);
  const [buffered, setBuffered] = useState(0);
  const [isFullscreen, setIsFullscreen] = useState(false);

  const hlsState = useHls({
    manifestUrl,
    videoRef,
    onError,
    onReady: () => {
      const video = videoRef.current;
      if (video && video.duration) {
        setDuration(video.duration);
      }
    },
  });

  const updateTime = useCallback(() => {
    const video = videoRef.current;
    if (!video) return;
    setCurrentTime(video.currentTime);
    setDuration(video.duration || 0);

    if (video.buffered.length > 0) {
      const end = video.buffered.end(video.buffered.length - 1);
      setBuffered(end);
    }
  }, []);

  const handlePlay = useCallback(() => setIsPlaying(true), []);
  const handlePause = useCallback(() => setIsPlaying(false), []);
  const handleVolumeChange = useCallback(() => {
    const video = videoRef.current;
    if (!video) return;
    setVolumeState(video.volume);
    setIsMuted(video.muted);
  }, []);

  const togglePlay = useCallback(() => {
    const video = videoRef.current;
    if (!video) return;
    if (video.paused || video.ended) {
      video.play().catch(() => {});
    } else {
      video.pause();
    }
  }, []);

  const play = useCallback(() => {
    videoRef.current?.play().catch(() => {});
  }, []);

  const pause = useCallback(() => {
    if (videoRef.current) {
      videoRef.current.pause();
    }
  }, []);

  const seek = useCallback((time: number) => {
    const video = videoRef.current;
    if (!video) return;
    const clamped = Math.max(0, Math.min(time, video.duration || time));
    video.currentTime = clamped;
    setCurrentTime(clamped);
  }, []);

  const seekRelative = useCallback(
    (delta: number) => {
      const video = videoRef.current;
      if (!video) return;
      seek(video.currentTime + delta);
    },
    [seek]
  );

  const setVolume = useCallback((nextVolume: number) => {
    const video = videoRef.current;
    if (!video) return;
    const clamped = Math.max(0, Math.min(1, nextVolume));
    video.volume = clamped;
    video.muted = clamped === 0;
    setVolumeState(clamped);
    setIsMuted(clamped === 0);
  }, []);

  const toggleMute = useCallback(() => {
    const video = videoRef.current;
    if (!video) return;
    video.muted = !video.muted;
    setIsMuted(video.muted);
  }, []);

  const changeVolumeRelative = useCallback(
    (delta: number) => {
      setVolume(volume + delta);
    },
    [setVolume, volume]
  );

  const toggleFullscreen = useCallback(async () => {
    const container = containerRef.current;
    if (!container) return;

    try {
      if (!document.fullscreenElement) {
        await container.requestFullscreen?.();
      } else {
        await document.exitFullscreen?.();
      }
    } catch {
      // Ignore fullscreen errors
    }
  }, []);

  useEffect(() => {
    const video = videoRef.current;
    if (!video) return;

    video.addEventListener('timeupdate', updateTime);
    video.addEventListener('progress', updateTime);
    video.addEventListener('play', handlePlay);
    video.addEventListener('pause', handlePause);
    video.addEventListener('volumechange', handleVolumeChange);

    const fullscreenHandler = () => {
      setIsFullscreen(!!document.fullscreenElement);
    };
    document.addEventListener('fullscreenchange', fullscreenHandler);

    return () => {
      video.removeEventListener('timeupdate', updateTime);
      video.removeEventListener('progress', updateTime);
      video.removeEventListener('play', handlePlay);
      video.removeEventListener('pause', handlePause);
      video.removeEventListener('volumechange', handleVolumeChange);
      document.removeEventListener('fullscreenchange', fullscreenHandler);
    };
  }, [updateTime, handlePlay, handlePause, handleVolumeChange]);

  useKeyboardShortcuts({
    videoRef,
    onTogglePlay: togglePlay,
    onSeek: seekRelative,
    onVolumeChange: changeVolumeRelative,
    onToggleMute: toggleMute,
    onToggleFullscreen: toggleFullscreen,
  });

  const value = useMemo<PlayerContextValue>(
    () => ({
      ...hlsState,
      videoRef,
      containerRef,
      isPlaying,
      isMuted,
      isFullscreen,
      volume,
      currentTime,
      duration,
      buffered,
      togglePlay,
      play,
      pause,
      seek,
      seekRelative,
      setVolume,
      toggleMute,
      changeVolumeRelative,
      toggleFullscreen,
      formattedCurrentTime: formatDuration(currentTime),
      formattedDuration: formatDuration(duration),
      progressPercent: duration ? (currentTime / duration) * 100 : 0,
      bufferedPercent: duration ? (buffered / duration) * 100 : 0,
    }),
    [
      hlsState,
      isPlaying,
      isMuted,
      isFullscreen,
      volume,
      currentTime,
      duration,
      buffered,
      togglePlay,
      play,
      pause,
      seek,
      seekRelative,
      setVolume,
      toggleMute,
      changeVolumeRelative,
      toggleFullscreen,
    ]
  );

  return (
    <PlayerContext.Provider value={value}>{children}</PlayerContext.Provider>
  );
}

export function usePlayer() {
  const context = useContext(PlayerContext);
  if (!context) {
    throw new Error('usePlayer must be used within a PlayerProvider');
  }
  return context;
}
