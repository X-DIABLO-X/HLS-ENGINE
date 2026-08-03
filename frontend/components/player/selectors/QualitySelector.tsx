'use client';

import { usePlayer } from '../PlayerProvider';

export function QualitySelector() {
  const { levels, currentLevel, setLevel } = usePlayer();

  if (levels.length === 0) return null;

  return (
    <div className="flex flex-col gap-1">
      <span className="px-3 py-1 text-xs font-medium uppercase tracking-wide text-white/60">
        Quality
      </span>
      <button
        type="button"
        onClick={() => setLevel(-1)}
        className={`px-3 py-2 text-left text-sm transition-colors hover:bg-white/10 ${
          currentLevel === -1 ? 'text-accent' : 'text-white'
        }`}
        aria-pressed={currentLevel === -1}
      >
        Auto
      </button>
      {levels.map((level, index) => (
        <button
          key={level.id}
          type="button"
          onClick={() => setLevel(index)}
          className={`px-3 py-2 text-left text-sm transition-colors hover:bg-white/10 ${
            currentLevel === index ? 'text-accent' : 'text-white'
          }`}
          aria-pressed={currentLevel === index}
        >
          {level.height}p
          {level.bitrate > 0 && (
            <span className="ml-2 text-white/50">
              {Math.round(level.bitrate / 1000)} kbps
            </span>
          )}
        </button>
      ))}
    </div>
  );
}
