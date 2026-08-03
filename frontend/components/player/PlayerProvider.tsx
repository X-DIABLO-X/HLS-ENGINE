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
import {
  defaultSubtitleAppearance,
  SubtitleAppearance,
} from './subtitleAppearance';

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
  subtitleAppearance: SubtitleAppearance;
  setSubtitleAppearance: (appearance: Partial<SubtitleAppearance>) => void;
  resetSubtitleAppearance: () => void;
  formattedCurrentTime: string;
  formattedDuration: string;
  progressPercent: number;
  bufferedPercent: number;
}

const PlayerContext = createContext<PlayerContextValue | null>(null);
const subtitleAppearanceStorageKey = 'hls-engine:subtitle-appearance';

function initialSubtitleAppearance(): SubtitleAppearance {
  if (typeof window === 'undefined') return defaultSubtitleAppearance;
  try {
    const stored = JSON.parse(
      window.localStorage.getItem(subtitleAppearanceStorageKey) || '{}'
    );
    return {
      ...defaultSubtitleAppearance,
      ...(stored && typeof stored === 'object' ? stored : {}),
    };
  } catch {
    return defaultSubtitleAppearance;
  }
}

interface PlayerProviderProps {
  children: React.ReactNode;
  manifestUrl: string;
  originalResolution?: { width: number; height: number };
  onError?: (error: Error) => void;
}

export function PlayerProvider({
  children,
  manifestUrl,
  originalResolution,
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
  const [subtitleAppearance, setSubtitleAppearanceState] =
    useState<SubtitleAppearance>(initialSubtitleAppearance);

  const hlsState = useHls({
    manifestUrl,
    videoRef,
    originalResolution,
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

  const setSubtitleAppearance = useCallback(
    (nextAppearance: Partial<SubtitleAppearance>) => {
      setSubtitleAppearanceState((current) => ({
        ...current,
        ...nextAppearance,
      }));
    },
    []
  );

  const resetSubtitleAppearance = useCallback(() => {
    setSubtitleAppearanceState(defaultSubtitleAppearance);
  }, []);

  useEffect(() => {
    try {
      window.localStorage.setItem(
        subtitleAppearanceStorageKey,
        JSON.stringify(subtitleAppearance)
      );
    } catch {
      // Styling is still usable when browser storage is unavailable.
    }

    const video = videoRef.current;
    if (!video) return;
    const background = {
      transparent: 'transparent',
      shadow: 'rgba(0, 0, 0, 0.58)',
      solid: 'rgba(0, 0, 0, 0.9)',
    }[subtitleAppearance.background];
    const fontSize = {
      small: '80%',
      medium: '100%',
      large: '125%',
    }[subtitleAppearance.fontSize];
    const cueLine = {
      bottom: 88,
      middle: 50,
      top: 12,
    }[subtitleAppearance.position];

    video.style.setProperty('--subtitle-color', subtitleAppearance.color);
    video.style.setProperty('--subtitle-background', background);
    video.style.setProperty('--subtitle-font-size', fontSize);

    const placeCue = (cue: VTTCue) => {
      cue.snapToLines = false;
      cue.line = cueLine;
      cue.lineAlign = 'center';
      cue.position = 50;
      cue.positionAlign = 'center';
    };
    const placeTrackCues = (track: TextTrack) => {
      for (const rawCue of Array.from(track.cues || [])) {
        placeCue(rawCue as VTTCue);
      }
    };
    const placeKnownCues = () => {
      for (const track of Array.from(video.textTracks)) {
        placeTrackCues(track);
      }
    };
    const cleanupTrackListeners: Array<() => void> = [];
    const listenToTrack = (track: TextTrack) => {
      const handleCueChange = () => placeTrackCues(track);
      placeTrackCues(track);
      // A cuechange is dispatched as a subtitle becomes active.  This keeps
      // its coordinates in place before the browser paints it, unlike the
      // old timeupdate handler which visibly moved an already shown cue.
      track.addEventListener('cuechange', handleCueChange);
      cleanupTrackListeners.push(() =>
        track.removeEventListener('cuechange', handleCueChange)
      );
    };
    const handleAddTrack = (event: TrackEvent) => {
      if (event.track) {
        listenToTrack(event.track);
      }
    };

    for (const track of Array.from(video.textTracks)) {
      listenToTrack(track);
    }
    placeKnownCues();
    video.textTracks.addEventListener('addtrack', handleAddTrack);
    video.addEventListener('loadedmetadata', placeKnownCues);

    return () => {
      video.textTracks.removeEventListener('addtrack', handleAddTrack);
      video.removeEventListener('loadedmetadata', placeKnownCues);
      cleanupTrackListeners.forEach((cleanup) => cleanup());
    };
  }, [subtitleAppearance]);

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
      subtitleAppearance,
      setSubtitleAppearance,
      resetSubtitleAppearance,
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
      subtitleAppearance,
      setSubtitleAppearance,
      resetSubtitleAppearance,
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
