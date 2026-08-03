'use client';

import { useEffect, useState } from 'react';
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
import { Settings, Save, Loader2, CheckCircle, AlertCircle } from 'lucide-react';

export default function SettingsPage() {
  const [settings, setSettings] = useState<TranscodingSettings | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    getTranscodingSettings()
      .then(setSettings)
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!settings) return;
    setSaving(true);
    setSaved(false);
    setError(null);
    try {
      await updateTranscodingSettings(settings);
      setSaved(true);
      setTimeout(() => setSaved(false), 3000);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  const toggleQuality = (quality: number) => {
    if (!settings) return;
    const qualities = settings.qualities.includes(quality)
      ? settings.qualities.filter((q) => q !== quality)
      : [...settings.qualities, quality].sort((a, b) => b - a);
    setSettings({ ...settings, qualities });
  };

  return (
    <AuthGuard>
      <div className="mx-auto max-w-2xl">
        <div className="mb-8 flex items-center gap-3">
          <div className="rounded-xl bg-accent/10 p-3">
            <Settings className="h-6 w-6 text-accent" />
          </div>
          <div>
            <h1 className="text-3xl font-bold tracking-tight">
              Transcoding Settings
            </h1>
            <p className="mt-1 text-muted-foreground">
              Configure default video quality, audio, and segment options.
            </p>
          </div>
        </div>

        {loading && (
          <div className="flex items-center justify-center gap-3 py-20">
            <Loader2 className="h-8 w-8 animate-spin text-accent" />
            <span className="text-muted-foreground">Loading settings…</span>
          </div>
        )}

        {error && (
          <div className="mb-4 rounded-xl border border-red-500/30 bg-red-500/10 p-4">
            <div className="flex items-center gap-2">
              <AlertCircle className="h-5 w-5 text-red-400" />
              <span className="text-sm text-red-400">{error}</span>
            </div>
          </div>
        )}

        {saved && (
          <div className="mb-4 rounded-xl border border-green-500/30 bg-green-500/10 p-4">
            <div className="flex items-center gap-2">
              <CheckCircle className="h-5 w-5 text-green-400" />
              <span className="text-sm text-green-400">Settings saved successfully</span>
            </div>
          </div>
        )}

        {settings && (
          <form onSubmit={handleSave} className="space-y-6">
            {/* Video Qualities */}
            <div className="rounded-xl border border-border bg-muted p-6">
              <h2 className="mb-1 text-lg font-semibold">Video Qualities</h2>
              <p className="mb-4 text-sm text-muted-foreground">
                Select which resolutions to generate. Higher resolutions take longer to transcode.
              </p>
              <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
                {QUALITY_OPTIONS.map((opt) => (
                  <label
                    key={opt.value}
                    className={`flex cursor-pointer items-center gap-2 rounded-lg border p-3 text-sm transition-colors ${
                      settings.qualities.includes(opt.value)
                        ? 'border-accent bg-accent/10 text-foreground'
                        : 'border-border bg-background text-muted-foreground hover:border-white/30'
                    }`}
                  >
                    <input
                      type="checkbox"
                      checked={settings.qualities.includes(opt.value)}
                      onChange={() => toggleQuality(opt.value)}
                      className="h-4 w-4 accent-accent"
                    />
                    {opt.label}
                  </label>
                ))}
              </div>
            </div>

            {/* Audio Settings */}
            <div className="rounded-xl border border-border bg-muted p-6">
              <h2 className="mb-1 text-lg font-semibold">Audio Settings</h2>
              <p className="mb-4 text-sm text-muted-foreground">
                Configure audio bitrate and channel layout for all tracks.
              </p>
              <div className="grid gap-4 sm:grid-cols-2">
                <div>
                  <label className="mb-1.5 block text-sm font-medium">
                    Audio Bitrate
                  </label>
                  <select
                    value={settings.audio_bitrate_kbps}
                    onChange={(e) =>
                      setSettings({
                        ...settings,
                        audio_bitrate_kbps: Number(e.target.value),
                      })
                    }
                    className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-accent"
                  >
                    {AUDIO_BITRATE_OPTIONS.map((opt) => (
                      <option key={opt.value} value={opt.value}>
                        {opt.label}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="mb-1.5 block text-sm font-medium">
                    Audio Channels
                  </label>
                  <select
                    value={settings.audio_channels}
                    onChange={(e) =>
                      setSettings({
                        ...settings,
                        audio_channels: Number(e.target.value),
                      })
                    }
                    className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-accent"
                  >
                    {AUDIO_CHANNEL_OPTIONS.map((opt) => (
                      <option key={opt.value} value={opt.value}>
                        {opt.label}
                      </option>
                    ))}
                  </select>
                </div>
              </div>
            </div>

            {/* Segment & Encoder */}
            <div className="rounded-xl border border-border bg-muted p-6">
              <h2 className="mb-1 text-lg font-semibold">Segment & Encoder</h2>
              <p className="mb-4 text-sm text-muted-foreground">
                Control HLS segment duration and encoder settings.
              </p>
              <div className="grid gap-4 sm:grid-cols-2">
                <div>
                  <label className="mb-1.5 block text-sm font-medium">
                    Segment Duration (seconds)
                  </label>
                  <input
                    type="number"
                    min={2}
                    max={30}
                    value={settings.segment_duration_sec}
                    onChange={(e) =>
                      setSettings({
                        ...settings,
                        segment_duration_sec: Number(e.target.value),
                      })
                    }
                    className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-accent"
                  />
                </div>
                <div>
                  <label className="mb-1.5 block text-sm font-medium">
                    Video Encoder Preset
                  </label>
                  <select
                    value={settings.video_preset}
                    onChange={(e) =>
                      setSettings({ ...settings, video_preset: e.target.value })
                    }
                    className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-accent"
                  >
                    {PRESET_OPTIONS.map((opt) => (
                      <option key={opt.value} value={opt.value}>
                        {opt.label}
                      </option>
                    ))}
                  </select>
                </div>
              </div>
              <label className="mt-4 flex cursor-pointer items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={settings.force_cpu}
                  onChange={(e) =>
                    setSettings({ ...settings, force_cpu: e.target.checked })
                  }
                  className="h-4 w-4 accent-accent"
                />
                <span>
                  Force CPU encoding (disable GPU/NVENC)
                </span>
              </label>
            </div>

            <button
              type="submit"
              disabled={saving}
              className="flex w-full items-center justify-center gap-2 rounded-lg bg-accent px-4 py-2.5 text-sm font-medium text-white transition-colors hover:bg-accent/90 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {saving ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Saving…
                </>
              ) : (
                <>
                  <Save className="h-4 w-4" />
                  Save Settings
                </>
              )}
            </button>
          </form>
        )}
      </div>
    </AuthGuard>
  );
}
