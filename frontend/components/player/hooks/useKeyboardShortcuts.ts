import { useEffect } from 'react';

interface UseKeyboardShortcutsOptions {
  videoRef: React.RefObject<HTMLVideoElement | null>;
  isEnabled?: boolean;
  onTogglePlay?: () => void;
  onSeek?: (seconds: number) => void;
  onVolumeChange?: (delta: number) => void;
  onToggleMute?: () => void;
  onToggleFullscreen?: () => void;
}

export function useKeyboardShortcuts(options: UseKeyboardShortcutsOptions): void {
  const {
    videoRef,
    isEnabled = true,
    onTogglePlay,
    onSeek,
    onVolumeChange,
    onToggleMute,
    onToggleFullscreen,
  } = options;

  useEffect(() => {
    if (!isEnabled) return;

    const handler = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement;
      const isTyping =
        target.tagName === 'INPUT' ||
        target.tagName === 'TEXTAREA' ||
        target.isContentEditable;
      if (isTyping) return;

      const video = videoRef.current;
      if (!video) return;

      switch (event.key) {
        case ' ':
        case 'k':
        case 'K':
          event.preventDefault();
          onTogglePlay?.();
          break;
        case 'ArrowRight':
        case 'l':
        case 'L':
          event.preventDefault();
          onSeek?.(10);
          break;
        case 'ArrowLeft':
        case 'j':
        case 'J':
          event.preventDefault();
          onSeek?.(-10);
          break;
        case 'ArrowUp':
          event.preventDefault();
          onVolumeChange?.(0.1);
          break;
        case 'ArrowDown':
          event.preventDefault();
          onVolumeChange?.(-0.1);
          break;
        case 'm':
        case 'M':
          event.preventDefault();
          onToggleMute?.();
          break;
        case 'f':
        case 'F':
          event.preventDefault();
          onToggleFullscreen?.();
          break;
        default:
          break;
      }
    };

    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [
    isEnabled,
    onSeek,
    onToggleFullscreen,
    onToggleMute,
    onTogglePlay,
    onVolumeChange,
    videoRef,
  ]);
}
