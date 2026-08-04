'use client';

import { PlayPauseButton } from './PlayPauseButton';
import { SeekBar } from './SeekBar';
import { VolumeControl } from './VolumeControl';
import { TimeDisplay } from './TimeDisplay';
import { FullscreenButton } from './FullscreenButton';
import { SettingsMenu } from './SettingsMenu';
import { SkipButton } from './SkipButton';
import { ChevronLeft, ChevronRight } from 'lucide-react';

interface ControlBarProps {
  previous?: { title: string; onNavigate: () => void };
  next?: { title: string; onNavigate: () => void };
}

export function ControlBar({ previous, next }: ControlBarProps) {
  return (
    <div className="player-ui flex flex-col gap-2 bg-gradient-to-t from-black/80 via-black/50 to-transparent px-3 pb-3 pt-8 transition-opacity duration-300 md:px-4 md:pb-4">
      <SeekBar />
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-1">
          <PlayPauseButton />
          {previous && (
            <button type="button" onClick={previous.onNavigate} className="rounded-md p-2 text-white transition-colors hover:bg-white/10 focus-visible:ring-2 focus-visible:ring-white" aria-label={`Previous episode: ${previous.title}`} title={`Previous: ${previous.title}`}>
              <ChevronLeft className="h-6 w-6" />
            </button>
          )}
          <SkipButton direction="backward" />
          <SkipButton direction="forward" />
          {next && (
            <button type="button" onClick={next.onNavigate} className="rounded-md p-2 text-white transition-colors hover:bg-white/10 focus-visible:ring-2 focus-visible:ring-white" aria-label={`Next episode: ${next.title}`} title={`Next: ${next.title}`}>
              <ChevronRight className="h-6 w-6" />
            </button>
          )}
          <VolumeControl />
          <TimeDisplay />
        </div>
        <div className="flex items-center gap-1">
          <SettingsMenu />
          <FullscreenButton />
        </div>
      </div>
    </div>
  );
}
