'use client';

import { PlayPauseButton } from './PlayPauseButton';
import { SeekBar } from './SeekBar';
import { VolumeControl } from './VolumeControl';
import { TimeDisplay } from './TimeDisplay';
import { FullscreenButton } from './FullscreenButton';
import { SettingsMenu } from './SettingsMenu';

export function ControlBar() {
  return (
    <div className="player-ui flex flex-col gap-2 bg-gradient-to-t from-black/80 via-black/50 to-transparent px-3 pb-3 pt-8 transition-opacity duration-300 md:px-4 md:pb-4">
      <SeekBar />
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-1">
          <PlayPauseButton />
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
