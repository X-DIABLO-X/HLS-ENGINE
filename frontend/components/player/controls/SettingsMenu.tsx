'use client';

import { useState } from 'react';
import { Settings } from 'lucide-react';
import { usePlayer } from '../PlayerProvider';
import { QualitySelector } from '../selectors/QualitySelector';
import { AudioTrackSelector } from '../selectors/AudioTrackSelector';
import { SubtitleSelector } from '../selectors/SubtitleSelector';

export function SettingsMenu() {
  const { levels, audioTracks, subtitles } = usePlayer();
  const [isOpen, setIsOpen] = useState(false);

  const hasOptions =
    levels.length > 0 || audioTracks.length > 1 || subtitles.length > 0;

  if (!hasOptions) return null;

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setIsOpen((prev) => !prev)}
        className={`rounded-full p-2 text-white transition-colors hover:bg-white/10 ${
          isOpen ? 'bg-white/10' : ''
        }`}
        aria-label="Settings"
        aria-expanded={isOpen}
        aria-haspopup="menu"
        title="Settings"
      >
        <Settings className="h-5 w-5" />
      </button>

      {isOpen && (
        <>
          <div
            className="fixed inset-0 z-40"
            onClick={() => setIsOpen(false)}
            aria-hidden="true"
          />
          <div
            role="menu"
            className="absolute bottom-full right-0 z-50 mb-2 w-56 rounded-lg border border-border bg-black/95 p-2 shadow-xl backdrop-blur-sm"
          >
            <QualitySelector />
            {audioTracks.length > 1 && (
              <div className="my-2 border-t border-white/10" />
            )}
            <AudioTrackSelector />
            {subtitles.length > 0 && (
              <div className="my-2 border-t border-white/10" />
            )}
            <SubtitleSelector />
          </div>
        </>
      )}
    </div>
  );
}
