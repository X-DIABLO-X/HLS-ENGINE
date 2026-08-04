'use client';

import { useState } from 'react';
import { RotateCcw, SlidersHorizontal } from 'lucide-react';
import { usePlayer } from '../PlayerProvider';
import {
  SubtitleBackground,
  SubtitleFontSize,
  SubtitlePosition,
  subtitleColors,
} from '../subtitleAppearance';

const positions: { value: SubtitlePosition; label: string }[] = [
  { value: 'bottom', label: 'Bottom' },
  { value: 'middle', label: 'Middle' },
  { value: 'top', label: 'Top' },
];

const backgrounds: { value: SubtitleBackground; label: string }[] = [
  { value: 'transparent', label: 'None' },
  { value: 'shadow', label: 'Shadow' },
  { value: 'solid', label: 'Solid' },
];

const fontSizes: { value: SubtitleFontSize; label: string }[] = [
  { value: 'small', label: 'Small' },
  { value: 'medium', label: 'Medium' },
  { value: 'large', label: 'Large' },
];

interface SegmentedOptionsProps<T extends string> {
  label: string;
  options: { value: T; label: string }[];
  value: T;
  onChange: (value: T) => void;
}

function SegmentedOptions<T extends string>({
  label,
  options,
  value,
  onChange,
}: SegmentedOptionsProps<T>) {
  return (
    <div>
      <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-wide text-white/45">
        {label}
      </span>
      <div className="grid grid-cols-3 gap-1 rounded-lg bg-white/5 p-1">
        {options.map((option) => (
          <button
            key={option.value}
            type="button"
            onClick={() => onChange(option.value)}
            className={`rounded-md px-1 py-1.5 text-xs transition-colors ${
              value === option.value
                ? 'bg-white/15 text-white'
                : 'text-white/55 hover:bg-white/10 hover:text-white'
            }`}
          >
            {option.label}
          </button>
        ))}
      </div>
    </div>
  );
}

export function SubtitleSelector() {
  const {
    subtitles,
    currentSubtitleTrack,
    setSubtitleTrack,
    subtitleAppearance,
    setSubtitleAppearance,
    resetSubtitleAppearance,
  } = usePlayer();
  const [showAppearance, setShowAppearance] = useState(false);

  if (subtitles.length === 0) return null;

  return (
    <div className="flex flex-col gap-1 p-1">
      <button
        type="button"
        onClick={() => setShowAppearance((current) => !current)}
        className={`mb-1 flex items-center justify-between rounded-md px-3 py-2 text-left text-xs font-semibold uppercase tracking-wide transition-colors ${
          showAppearance
            ? 'bg-white/10 text-accent'
            : 'text-white/60 hover:bg-white/10 hover:text-white'
        }`}
        aria-expanded={showAppearance}
      >
        Appearance
        <SlidersHorizontal className="h-4 w-4" aria-hidden="true" />
      </button>

      {showAppearance && (
        <section className="mb-2 space-y-3 rounded-lg border border-white/10 bg-white/[0.035] p-3" aria-label="Subtitle appearance">
          <SegmentedOptions
            label="Position"
            options={positions}
            value={subtitleAppearance.position}
            onChange={(position) => setSubtitleAppearance({ position })}
          />
          <div>
            <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-wide text-white/45">
              Text color
            </span>
            <div className="flex gap-2">
              {subtitleColors.map((color) => (
                <button
                  key={color.value}
                  type="button"
                  onClick={() => setSubtitleAppearance({ color: color.value })}
                  className={`h-7 flex-1 rounded-md border transition-transform hover:scale-[1.03] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent ${
                    subtitleAppearance.color === color.value
                      ? 'border-accent ring-1 ring-accent'
                      : 'border-white/20'
                  }`}
                  style={{ backgroundColor: color.value }}
                  aria-label={`${color.label} subtitles`}
                  title={color.label}
                />
              ))}
            </div>
          </div>
          <SegmentedOptions
            label="Background"
            options={backgrounds}
            value={subtitleAppearance.background}
            onChange={(background) => setSubtitleAppearance({ background })}
          />
          <SegmentedOptions
            label="Text size"
            options={fontSizes}
            value={subtitleAppearance.fontSize}
            onChange={(fontSize) => setSubtitleAppearance({ fontSize })}
          />
          <button
            type="button"
            onClick={resetSubtitleAppearance}
            className="flex w-full items-center justify-center gap-1.5 rounded-md border border-white/15 px-3 py-2 text-xs font-medium text-white/70 transition-colors hover:bg-white/10 hover:text-white"
          >
            <RotateCcw className="h-3.5 w-3.5" aria-hidden="true" />
            Reset appearance
          </button>
        </section>
      )}

      <button
        type="button"
        onClick={() => setSubtitleTrack(-1)}
        className={`rounded-md px-3 py-2.5 text-left text-sm transition-colors hover:bg-white/10 ${
          currentSubtitleTrack === -1 ? 'bg-accent/10 text-accent' : 'text-white'
        }`}
        aria-pressed={currentSubtitleTrack === -1}
      >
        Off
      </button>
      {subtitles.map((track, index) => (
        <button
          key={track.id}
          type="button"
          onClick={() => setSubtitleTrack(index)}
          className={`rounded-md px-3 py-2.5 text-left text-sm transition-colors hover:bg-white/10 ${
            currentSubtitleTrack === index ? 'bg-accent/10 text-accent' : 'text-white'
          }`}
          aria-pressed={currentSubtitleTrack === index}
        >
          {track.name}
        </button>
      ))}
    </div>
  );
}
