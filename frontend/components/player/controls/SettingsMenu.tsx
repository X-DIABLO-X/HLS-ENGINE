'use client';

import { useState } from 'react';
import { Settings } from 'lucide-react';
import { usePlayer } from '../PlayerProvider';
import { QualitySelector } from '../selectors/QualitySelector';
import { AudioTrackSelector } from '../selectors/AudioTrackSelector';
import { SubtitleSelector } from '../selectors/SubtitleSelector';

type SettingsTab = 'quality' | 'audio' | 'subtitles';

const tabLabels: Record<SettingsTab, string> = {
  quality: 'Quality',
  audio: 'Audio',
  subtitles: 'Subtitles',
};

export function SettingsMenu() {
  const { levels, audioTracks, subtitles } = usePlayer();
  const [isOpen, setIsOpen] = useState(false);
  const [selectedTab, setSelectedTab] = useState<SettingsTab>('quality');

  const tabs = (['quality', 'audio', 'subtitles'] as SettingsTab[]).filter(
    (tab) =>
      (tab === 'quality' && levels.length > 0) ||
      (tab === 'audio' && audioTracks.length > 1) ||
      (tab === 'subtitles' && subtitles.length > 0)
  );
  const activeTab = tabs.includes(selectedTab) ? selectedTab : tabs[0];

  if (tabs.length === 0) return null;

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setIsOpen((prev) => !prev)}
        className={`rounded-full p-2 text-white transition-colors hover:bg-white/10 focus-visible:ring-2 focus-visible:ring-white ${
          isOpen ? 'bg-white/10' : ''
        }`}
        aria-label="Player settings"
        aria-expanded={isOpen}
        aria-haspopup="dialog"
        title="Player settings"
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
          <section
            role="dialog"
            aria-label="Player settings"
            className="absolute bottom-full right-0 z-50 mb-3 w-[min(19rem,calc(100vw-2rem))] overflow-hidden rounded-xl border border-white/10 bg-black/95 shadow-2xl backdrop-blur-md"
          >
            <div className="border-b border-white/10 px-2 pt-2">
              <div role="tablist" aria-label="Player settings categories" className="flex gap-1">
                {tabs.map((tab) => (
                  <button
                    key={tab}
                    id={`player-settings-tab-${tab}`}
                    type="button"
                    role="tab"
                    aria-selected={activeTab === tab}
                    aria-controls={`player-settings-panel-${tab}`}
                    onClick={() => setSelectedTab(tab)}
                    className={`relative flex-1 rounded-t-lg px-2 py-2.5 text-xs font-semibold uppercase tracking-wide transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-white ${
                      activeTab === tab
                        ? 'bg-white/10 text-accent'
                        : 'text-white/55 hover:bg-white/5 hover:text-white'
                    }`}
                  >
                    {tabLabels[tab]}
                    {activeTab === tab && (
                      <span className="absolute inset-x-3 bottom-0 h-0.5 rounded-full bg-accent" />
                    )}
                  </button>
                ))}
              </div>
            </div>
            <div
              id={`player-settings-panel-${activeTab}`}
              role="tabpanel"
              aria-labelledby={`player-settings-tab-${activeTab}`}
              className="max-h-[min(50vh,22rem)] overflow-y-auto p-2"
            >
              {activeTab === 'quality' && <QualitySelector />}
              {activeTab === 'audio' && <AudioTrackSelector />}
              {activeTab === 'subtitles' && <SubtitleSelector />}
            </div>
          </section>
        </>
      )}
    </div>
  );
}
