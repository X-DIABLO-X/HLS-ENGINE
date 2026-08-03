'use client';

import { usePlayer } from '../PlayerProvider';

const qualityPresets = [
  { width: 3840, label: '2160p' },
  { width: 1920, label: '1080p' },
  { width: 1280, label: '720p' },
  { width: 854, label: '480p' },
  { width: 640, label: '360p' },
  { width: 426, label: '240p' },
  { width: 256, label: '144p' },
];

function qualityLabel(width: number, height: number): string {
  const preset = qualityPresets.find((item) => Math.abs(item.width - width) <= 2);
  return preset ? `${preset.label} (${width}×${height})` : `${height}p (${width}×${height})`;
}

export function QualitySelector() {
  const { levels, currentLevel, activeLevel, setLevel } = usePlayer();

  if (levels.length === 0) return null;

  return (
    <div className="flex flex-col gap-1 p-1">
      <button
        type="button"
        onClick={() => setLevel(-1)}
        className={`flex items-center justify-between rounded-md px-3 py-2.5 text-left text-sm transition-colors hover:bg-white/10 ${
          currentLevel === -1 ? 'bg-accent/10 text-accent' : 'text-white'
        }`}
        aria-pressed={currentLevel === -1}
      >
        <span>Auto</span>
        {activeLevel >= 0 && levels[activeLevel] && (
          <span className="text-xs tabular-nums text-white/50">
            Playing {qualityLabel(levels[activeLevel].width, levels[activeLevel].height)}
          </span>
        )}
      </button>
      {levels.map((level, index) => (
        <button
          key={level.id}
          type="button"
          onClick={() => setLevel(index)}
          className={`flex items-center justify-between rounded-md px-3 py-2.5 text-left text-sm transition-colors hover:bg-white/10 ${
            currentLevel === index ? 'bg-accent/10 text-accent' : 'text-white'
          }`}
          aria-pressed={currentLevel === index}
        >
          <span>{qualityLabel(level.width, level.height)}</span>
          <span className="flex items-center gap-2 text-xs tabular-nums text-white/50">
            {activeLevel === index && currentLevel !== -1 && (
              <span className="rounded bg-white/10 px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-white/75">
                Playing
              </span>
            )}
            {level.bitrate > 0 && (
              <span>{Math.round(level.bitrate / 1000)} kbps</span>
            )}
          </span>
        </button>
      ))}
    </div>
  );
}
