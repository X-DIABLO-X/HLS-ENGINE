'use client';

import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { AuthGuard } from '@/components/auth/AuthGuard';
import {
  getTranscodingSettings,
  updateTranscodingSettings,
} from '@/lib/api';
import {
  TranscodingSettings,
  QUALITY_OPTIONS,
  AUDIO_BITRATE_OPTIONS,
  AUDIO_CHANNEL_OPTIONS,
  PRESET_OPTIONS,
} from '@/types/video';
import {
  AlertCircle,
  AudioLines,
  Boxes,
  CheckCircle2,
  ChevronRight,
  Cpu,
  Gauge,
  Loader2,
  Save,
  Settings2,
} from 'lucide-react';

type SettingsSection = 'video' | 'audio' | 'delivery';

const sections = [
  {
    id: 'video' as const,
    label: 'Video quality',
    description: 'Rendition ladder',
    icon: Boxes,
  },
  {
    id: 'audio' as const,
    label: 'Audio',
    description: 'Track defaults',
    icon: AudioLines,
  },
  {
    id: 'delivery' as const,
    label: 'Packaging & compute',
    description: 'HLS delivery',
    icon: Gauge,
  },
];

function settingsMatch(
  left: TranscodingSettings | null,
  right: TranscodingSettings | null
) {
  return JSON.stringify(left) === JSON.stringify(right);
}

export default function SettingsPage() {
  const [settings, setSettings] = useState<TranscodingSettings | null>(null);
  const [savedSettings, setSavedSettings] =
    useState<TranscodingSettings | null>(null);
  const [activeSection, setActiveSection] =
    useState<SettingsSection>('video');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    getTranscodingSettings()
      .then((result) => {
        setSettings(result);
        setSavedSettings(result);
      })
      .catch((err) => setError(err instanceof Error ? err.message : String(err)))
      .finally(() => setLoading(false));
  }, []);

  const hasChanges = !settingsMatch(settings, savedSettings);
  const selectedQualitySummary = useMemo(() => {
    if (!settings) return 'Loading…';
    return `${settings.qualities.length} selected`;
  }, [settings]);
  const audioSummary = useMemo(() => {
    if (!settings) return 'Loading…';
    const bitrate = AUDIO_BITRATE_OPTIONS.find(
      (option) => option.value === settings.audio_bitrate_kbps
    )?.label;
    return bitrate || `${settings.audio_bitrate_kbps} kbps`;
  }, [settings]);

  const sectionSummary: Record<SettingsSection, string> = {
    video: selectedQualitySummary,
    audio: audioSummary,
    delivery: settings?.force_cpu ? 'CPU encoding' : 'GPU preferred',
  };

  const handleSave = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!settings || !hasChanges) return;

    setSaving(true);
    setSaved(false);
    setError(null);
    try {
      const result = await updateTranscodingSettings(settings);
      setSettings(result);
      setSavedSettings(result);
      setSaved(true);
      window.setTimeout(() => setSaved(false), 3000);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  const toggleQuality = (quality: number) => {
    if (!settings) return;
    const qualities = settings.qualities.includes(quality)
      ? settings.qualities.filter((item) => item !== quality)
      : [...settings.qualities, quality].sort((a, b) => b - a);
    setSettings({ ...settings, qualities });
  };

  const sectionButtonClass = (section: SettingsSection, compact = false) =>
    `group flex ${compact ? 'min-w-max flex-1' : 'w-full'} items-center gap-3 rounded-lg border px-3 py-3 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent ${
      activeSection === section
        ? 'border-accent/45 bg-accent/10 text-foreground'
        : 'border-transparent text-muted-foreground hover:border-border hover:bg-muted/60 hover:text-foreground'
    }`;

  return (
    <AuthGuard>
      <div className="mx-auto max-w-6xl">
        <header className="mb-8 flex flex-col gap-4 border-b border-border/80 pb-6 sm:flex-row sm:items-end sm:justify-between">
          <div className="flex items-start gap-3">
            <div className="rounded-lg border border-accent/25 bg-accent/10 p-2.5">
              <Settings2 className="h-5 w-5 text-accent" />
            </div>
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.16em] text-accent">
                Delivery defaults
              </p>
              <h1 className="mt-1 text-3xl font-bold tracking-tight">
                Transcoding settings
              </h1>
              <p className="mt-1 text-sm text-muted-foreground">
                Shape how each upload is encoded, packaged, and delivered.
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <span
              className={`h-2 w-2 rounded-full ${
                hasChanges ? 'bg-amber-400' : 'bg-emerald-400'
              }`}
            />
            {hasChanges ? 'Unsaved changes' : 'All changes saved'}
          </div>
        </header>

        {loading && (
          <div className="flex min-h-80 items-center justify-center gap-3 rounded-xl border border-border bg-muted/50">
            <Loader2 className="h-6 w-6 animate-spin text-accent" />
            <span className="text-sm text-muted-foreground">Loading delivery defaults…</span>
          </div>
        )}

        {!loading && !settings && error && (
          <div className="rounded-xl border border-red-500/30 bg-red-500/10 p-5 text-sm text-red-300">
            <div className="flex items-center gap-2 font-medium">
              <AlertCircle className="h-5 w-5" />
              Unable to load transcoding settings
            </div>
            <p className="mt-2 text-red-300/80">{error}</p>
          </div>
        )}

        {settings && (
          <form onSubmit={handleSave}>
            <div className="mb-5 flex gap-2 overflow-x-auto border-b border-border pb-3 lg:hidden">
              {sections.map((section) => {
                const Icon = section.icon;
                return (
                  <button
                    key={section.id}
                    type="button"
                    onClick={() => setActiveSection(section.id)}
                    aria-pressed={activeSection === section.id}
                    className={sectionButtonClass(section.id, true)}
                  >
                    <Icon className="h-4 w-4 shrink-0" />
                    <span className="text-sm font-medium">{section.label}</span>
                  </button>
                );
              })}
            </div>

            <div className="grid gap-8 lg:grid-cols-[225px_minmax(0,1fr)]">
              <aside className="hidden lg:block">
                <nav aria-label="Transcoding setting sections" className="sticky top-24 space-y-1">
                  <p className="mb-3 px-3 text-xs font-semibold uppercase tracking-[0.14em] text-muted-foreground">
                    Delivery path
                  </p>
                  {sections.map((section, index) => {
                    const Icon = section.icon;
                    return (
                      <button
                        key={section.id}
                        type="button"
                        onClick={() => setActiveSection(section.id)}
                        aria-current={activeSection === section.id ? 'page' : undefined}
                        className={sectionButtonClass(section.id)}
                      >
                        <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-background/70 text-xs font-semibold text-muted-foreground group-hover:text-foreground">
                          {index + 1}
                        </span>
                        <span className="min-w-0 flex-1">
                          <span className="flex items-center gap-2 text-sm font-medium">
                            <Icon className="h-4 w-4" />
                            {section.label}
                          </span>
                          <span className="mt-0.5 block truncate text-xs text-muted-foreground">
                            {sectionSummary[section.id]}
                          </span>
                        </span>
                        <ChevronRight className="h-4 w-4 shrink-0 opacity-0 transition-opacity group-hover:opacity-100" />
                      </button>
                    );
                  })}
                </nav>
              </aside>

              <section className="min-w-0 rounded-xl border border-border bg-muted/55">
                {activeSection === 'video' && (
                  <div className="p-5 sm:p-7">
                    <SectionHeading
                      eyebrow="Stage 01"
                      title="Video quality ladder"
                      description="Choose the adaptive variants generated for each upload."
                    />
                    <div className="mb-6 flex gap-3 rounded-lg border border-accent/20 bg-accent/5 p-4 text-sm text-muted-foreground">
                      <Boxes className="mt-0.5 h-4 w-4 shrink-0 text-accent" />
                      <p>
                        <span className="font-medium text-foreground">Original is always included.</span>{' '}
                        It preserves the source dimensions and appears alongside the selected adaptive qualities.
                      </p>
                    </div>
                    <div className="grid gap-3 sm:grid-cols-2">
                      {QUALITY_OPTIONS.map((option) => {
                        const selected = settings.qualities.includes(option.value);
                        return (
                          <label
                            key={option.value}
                            className={`flex cursor-pointer items-center justify-between rounded-lg border p-4 transition-colors ${
                              selected
                                ? 'border-accent/70 bg-accent/10'
                                : 'border-border bg-background/70 hover:border-white/25'
                            }`}
                          >
                            <span className="flex items-center gap-3">
                              <input
                                type="checkbox"
                                checked={selected}
                                onChange={() => toggleQuality(option.value)}
                                className="h-4 w-4 accent-accent"
                              />
                              <span className="text-sm font-medium">{option.label}</span>
                            </span>
                            <span className="text-xs text-muted-foreground">
                              {option.value >= 1080 ? 'Higher fidelity' : 'Adaptive'}
                            </span>
                          </label>
                        );
                      })}
                    </div>
                  </div>
                )}

                {activeSection === 'audio' && (
                  <div className="p-5 sm:p-7">
                    <SectionHeading
                      eyebrow="Stage 02"
                      title="Audio defaults"
                      description="Apply a consistent bitrate and channel layout to every published track."
                    />
                    <div className="grid gap-5 sm:grid-cols-2">
                      <ControlField label="Audio bitrate" hint="Per normalized audio track">
                        <select
                          value={settings.audio_bitrate_kbps}
                          onChange={(event) =>
                            setSettings({
                              ...settings,
                              audio_bitrate_kbps: Number(event.target.value),
                            })
                          }
                          className="control-select"
                        >
                          {AUDIO_BITRATE_OPTIONS.map((option) => (
                            <option key={option.value} value={option.value}>
                              {option.label}
                            </option>
                          ))}
                        </select>
                      </ControlField>
                      <ControlField label="Channel layout" hint="Output layout for every track">
                        <select
                          value={settings.audio_channels}
                          onChange={(event) =>
                            setSettings({
                              ...settings,
                              audio_channels: Number(event.target.value),
                            })
                          }
                          className="control-select"
                        >
                          {AUDIO_CHANNEL_OPTIONS.map((option) => (
                            <option key={option.value} value={option.value}>
                              {option.label}
                            </option>
                          ))}
                        </select>
                      </ControlField>
                    </div>
                  </div>
                )}

                {activeSection === 'delivery' && (
                  <div className="p-5 sm:p-7">
                    <SectionHeading
                      eyebrow="Stage 03"
                      title="Packaging & compute"
                      description="Balance startup speed, segment overhead, and available encoding hardware."
                    />
                    <div className="grid gap-5 sm:grid-cols-2">
                      <ControlField label="Segment duration" hint="Between 2 and 30 seconds">
                        <div className="relative">
                          <input
                            type="number"
                            min={2}
                            max={30}
                            value={settings.segment_duration_sec}
                            onChange={(event) =>
                              setSettings({
                                ...settings,
                                segment_duration_sec: Number(event.target.value),
                              })
                            }
                            className="control-select pr-16"
                          />
                          <span className="pointer-events-none absolute inset-y-0 right-4 flex items-center text-sm text-muted-foreground">
                            seconds
                          </span>
                        </div>
                      </ControlField>
                      <ControlField label="Video encoder preset" hint="Trade speed against compression efficiency">
                        <select
                          value={settings.video_preset}
                          onChange={(event) =>
                            setSettings({ ...settings, video_preset: event.target.value })
                          }
                          className="control-select"
                        >
                          {PRESET_OPTIONS.map((option) => (
                            <option key={option.value} value={option.value}>
                              {option.label}
                            </option>
                          ))}
                        </select>
                      </ControlField>
                    </div>
                    <label className="mt-6 flex cursor-pointer items-start gap-3 rounded-lg border border-border bg-background/70 p-4 transition-colors hover:border-white/25">
                      <input
                        type="checkbox"
                        checked={settings.force_cpu}
                        onChange={(event) =>
                          setSettings({ ...settings, force_cpu: event.target.checked })
                        }
                        className="mt-0.5 h-4 w-4 accent-accent"
                      />
                      <span>
                        <span className="flex items-center gap-2 text-sm font-medium">
                          <Cpu className="h-4 w-4 text-accent" />
                          Force CPU encoding
                        </span>
                        <span className="mt-1 block text-sm text-muted-foreground">
                          Disable NVENC/GPU acceleration. Use only for troubleshooting or CPU-only workers; GPU remains preferred by default.
                        </span>
                      </span>
                    </label>
                  </div>
                )}
              </section>
            </div>

            <div className="sticky bottom-4 z-20 mt-6 rounded-xl border border-border bg-[#18181b]/95 p-3 backdrop-blur sm:flex sm:items-center sm:justify-between sm:gap-4 sm:p-4">
              <div className="mb-3 flex items-center gap-2 text-sm sm:mb-0">
                {error ? (
                  <>
                    <AlertCircle className="h-4 w-4 text-red-400" />
                    <span className="text-red-300">{error}</span>
                  </>
                ) : saved ? (
                  <>
                    <CheckCircle2 className="h-4 w-4 text-emerald-400" />
                    <span className="text-emerald-300">Delivery defaults saved</span>
                  </>
                ) : hasChanges ? (
                  <span className="text-muted-foreground">Review changes across sections, then save once.</span>
                ) : (
                  <span className="text-muted-foreground">Your delivery defaults are up to date.</span>
                )}
              </div>
              <button
                type="submit"
                disabled={!hasChanges || saving}
                className="flex w-full items-center justify-center gap-2 rounded-lg bg-accent px-4 py-2.5 text-sm font-semibold text-white transition-colors hover:bg-accent/90 disabled:cursor-not-allowed disabled:opacity-45 sm:w-auto"
              >
                {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                {saving ? 'Saving changes…' : 'Save changes'}
              </button>
            </div>
          </form>
        )}
      </div>
    </AuthGuard>
  );
}

function SectionHeading({
  eyebrow,
  title,
  description,
}: {
  eyebrow: string;
  title: string;
  description: string;
}) {
  return (
    <div className="mb-6">
      <p className="text-xs font-semibold uppercase tracking-[0.14em] text-accent">{eyebrow}</p>
      <h2 className="mt-2 text-xl font-semibold tracking-tight">{title}</h2>
      <p className="mt-1 text-sm text-muted-foreground">{description}</p>
    </div>
  );
}

function ControlField({
  label,
  hint,
  children,
}: {
  label: string;
  hint: string;
  children: ReactNode;
}) {
  return (
    <label className="block">
      <span className="block text-sm font-medium">{label}</span>
      <span className="mt-1 block text-xs text-muted-foreground">{hint}</span>
      <span className="mt-2 block">{children}</span>
    </label>
  );
}
