import { useEffect, useRef, useState } from 'react';
import Hls, { Level, MediaPlaylist } from 'hls.js';
import { AudioTrack, Rendition, Subtitle } from '@/types/video';
import { displayLanguage } from '../language';

export interface UseHlsOptions {
  manifestUrl: string;
  videoRef: React.RefObject<HTMLVideoElement | null>;
  originalResolution?: { width: number; height: number };
  onError?: (error: Error) => void;
  onReady?: () => void;
}

export interface UseHlsReturn {
  isLoading: boolean;
  error: Error | null;
  levels: Rendition[];
  audioTracks: AudioTrack[];
  subtitles: Subtitle[];
  /** User's quality choice: -1 means adaptive (Auto). */
  currentLevel: number;
  /** Rendition hls.js is actually decoding right now. */
  activeLevel: number;
  currentAudioTrack: number;
  currentSubtitleTrack: number;
  setLevel: (level: number) => void;
  setAudioTrack: (index: number) => void;
  setSubtitleTrack: (index: number) => void;
}

function mapRenditions(
  hlsLevels: Level[],
  originalResolution?: { width: number; height: number }
): Rendition[] {
  return hlsLevels.map((level, index) => ({
    id: `level-${index}`,
    width: level.width,
    height: level.height,
    bitrate: level.bitrate,
    codec: level.codecSet,
    frameRate: level.frameRate,
    isOriginal:
      originalResolution?.width === level.width &&
      originalResolution.height === level.height,
  }));
}

function mapAudioTracks(tracks: MediaPlaylist[]): AudioTrack[] {
  const seen = new Map<string, number>();
  return tracks.map((track, index) => ({
    id: `audio-${index}`,
    lang: track.lang || 'und',
    name: numberedLanguageName(track.lang, seen),
    default: track.default,
    autoselect: track.autoselect,
    forced: track.forced,
  }));
}

function mapSubtitles(tracks: MediaPlaylist[]): Subtitle[] {
  const seen = new Map<string, number>();
  return tracks.map((track, index) => ({
    id: `subtitle-${index}`,
    lang: track.lang || 'und',
    name: numberedLanguageName(track.lang, seen),
    default: track.default,
    forced: track.forced,
  }));
}

function numberedLanguageName(
  language: string | undefined,
  seen: Map<string, number>
): string {
  const key = (language || 'und').toLowerCase();
  const number = (seen.get(key) || 0) + 1;
  seen.set(key, number);
  const base = displayLanguage(language);
  return number === 1 ? base : `${base} ${number}`;
}

function isSubtitleError(data: { details?: unknown; frag?: { type?: unknown } }): boolean {
  return (
    data.frag?.type === 'subtitle' ||
    (typeof data.details === 'string' && data.details.toLowerCase().includes('subtitle'))
  );
}

export function useHls(options: UseHlsOptions): UseHlsReturn {
  const { manifestUrl, videoRef, originalResolution, onError, onReady } = options;
  const hlsRef = useRef<Hls | null>(null);
  // hls.js rebuilds its audio-track list when it parses or reloads a master
  // playlist. Keep an explicit user choice outside React state so that a
  // playlist refresh cannot silently put the player back on DEFAULT=YES.
  const preferredAudioTrackRef = useRef<number | null>(null);

  // Keep callbacks in refs so they don't destroy/recreate the Hls instance
  // on every parent render.
  const onErrorRef = useRef(onError);
  const onReadyRef = useRef(onReady);

  useEffect(() => {
    onErrorRef.current = onError;
    onReadyRef.current = onReady;
  }, [onError, onReady]);

  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const [levels, setLevels] = useState<Rendition[]>([]);
  const [audioTracks, setAudioTracks] = useState<AudioTrack[]>([]);
  const [subtitles, setSubtitles] = useState<Subtitle[]>([]);
  // Keep the selected quality distinct from the rendition being decoded.
  // In adaptive mode hls.js emits LEVEL_SWITCHED whenever it changes bitrate;
  // using that event to set currentLevel made Auto look like a manual choice.
  const [currentLevel, setCurrentLevel] = useState(-1);
  const [activeLevel, setActiveLevel] = useState(-1);
  const [currentAudioTrack, setCurrentAudioTrack] = useState(-1);
  const [currentSubtitleTrack, setCurrentSubtitleTrack] = useState(-1);

  useEffect(() => {
    const video = videoRef.current;
    if (!video) return;

    preferredAudioTrackRef.current = null;
    setIsLoading(true);
    setError(null);

    let hls: Hls;

    if (Hls.isSupported()) {
      hls = new Hls({
        enableWorker: true,
        lowLatencyMode: false,
        backBufferLength: 90,
        maxBufferLength: 60,
      });
      hlsRef.current = hls;

      hls.on(Hls.Events.MEDIA_ATTACHED, () => {
        hls.loadSource(manifestUrl);
      });

      hls.on(Hls.Events.MANIFEST_PARSED, (_event, data) => {
        setLevels(mapRenditions(data.levels, originalResolution));
        setCurrentLevel(-1);
        setActiveLevel(hls.currentLevel);
        setAudioTracks(mapAudioTracks(hls.audioTracks));
        setCurrentAudioTrack(hls.audioTrack);
        const subtitleTracks = hls.subtitleTracks;
        const defaultSubtitleTrack = subtitleTracks.findIndex(
          (track) => track.default
        );
        // hls.js leaves subtitles off unless instructed otherwise on some
        // browsers. Honour the manifest's DEFAULT=YES selection explicitly.
        if (defaultSubtitleTrack >= 0) {
          hls.subtitleTrack = defaultSubtitleTrack;
          setCurrentSubtitleTrack(defaultSubtitleTrack);
        }
        setSubtitles(mapSubtitles(subtitleTracks));
        setIsLoading(false);
        onReadyRef.current?.();
      });

      hls.on(Hls.Events.AUDIO_TRACKS_UPDATED, (_event, data) => {
        setAudioTracks(mapAudioTracks(data.audioTracks));

        const preferredTrack = preferredAudioTrackRef.current;
        if (
          preferredTrack !== null &&
          preferredTrack >= 0 &&
          preferredTrack < data.audioTracks.length &&
          hls.audioTrack !== preferredTrack
        ) {
          // A source reload selects the manifest default before it emits this
          // event. Restore the track selected from the settings menu.
          hls.audioTrack = preferredTrack;
        }
      });

      hls.on(Hls.Events.SUBTITLE_TRACKS_UPDATED, (_event, data) => {
        setSubtitles(mapSubtitles(data.subtitleTracks || []));
      });

      hls.on(Hls.Events.LEVEL_SWITCHED, (_event, data) => {
        setActiveLevel(data.level);
      });

      hls.on(Hls.Events.AUDIO_TRACK_SWITCHED, (_event, data) => {
        setCurrentAudioTrack(data.id);
      });

      hls.on(Hls.Events.SUBTITLE_TRACK_SWITCH, (_event, data) => {
        setCurrentSubtitleTrack(data.id);
      });

      hls.on(Hls.Events.ERROR, (_event, data) => {
        // Subtitle playlists are optional. A bad or expired subtitle request
        // must never turn a healthy A/V session into a fatal player error.
        if (isSubtitleError(data)) {
          hls.subtitleTrack = -1;
          setCurrentSubtitleTrack(-1);
          console.warn('Subtitle track was disabled after a loading error', data);
          return;
        }
        if (data.fatal) {
          let message = 'HLS playback error';
          switch (data.type) {
            case Hls.ErrorTypes.NETWORK_ERROR:
              message = 'Network error while loading stream';
              hls.startLoad();
              break;
            case Hls.ErrorTypes.MEDIA_ERROR:
              message = 'Media decoding error';
              hls.recoverMediaError();
              break;
            default:
              message = `Fatal HLS error: ${data.type}`;
              hls.destroy();
              break;
          }
          const err = new Error(message);
          setError(err);
          onErrorRef.current?.(err);
          setIsLoading(false);
        }
      });

      hls.attachMedia(video);
    } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
      // Native HLS fallback for Safari
      video.src = manifestUrl;
      video.addEventListener('loadedmetadata', () => {
        setIsLoading(false);
        onReadyRef.current?.();
      });
      video.addEventListener('error', () => {
        const err = new Error('Native HLS playback error');
        setError(err);
        onErrorRef.current?.(err);
        setIsLoading(false);
      });
    } else {
      const err = new Error('HLS playback is not supported in this browser');
      setError(err);
      onErrorRef.current?.(err);
      setIsLoading(false);
    }

    return () => {
      hls?.destroy();
      hlsRef.current = null;
    };
  }, [manifestUrl, originalResolution, videoRef]);

  const setLevel = (level: number) => {
    const hls = hlsRef.current;
    if (!hls) return;
    hls.nextLevel = level;
    setCurrentLevel(level);
  };

  const setAudioTrack = (index: number) => {
    const hls = hlsRef.current;
    if (!hls || index < 0 || index >= hls.audioTracks.length) return;
    preferredAudioTrackRef.current = index;
    hls.audioTrack = index;
    setCurrentAudioTrack(index);
  };

  const setSubtitleTrack = (index: number) => {
    const hls = hlsRef.current;
    if (!hls) return;
    hls.subtitleTrack = index;
    setCurrentSubtitleTrack(index);
  };

  return {
    isLoading,
    error,
    levels,
    audioTracks,
    subtitles,
    currentLevel,
    activeLevel,
    currentAudioTrack,
    currentSubtitleTrack,
    setLevel,
    setAudioTrack,
    setSubtitleTrack,
  };
}
