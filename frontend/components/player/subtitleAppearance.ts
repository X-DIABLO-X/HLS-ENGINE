export type SubtitlePosition = 'bottom' | 'middle' | 'top';
export type SubtitleBackground = 'transparent' | 'shadow' | 'solid';
export type SubtitleFontSize = 'small' | 'medium' | 'large';

export interface SubtitleAppearance {
  position: SubtitlePosition;
  color: string;
  background: SubtitleBackground;
  fontSize: SubtitleFontSize;
}

export const defaultSubtitleAppearance: SubtitleAppearance = {
  position: 'bottom',
  color: '#ffffff',
  background: 'shadow',
  fontSize: 'medium',
};

export const subtitleColors = [
  { label: 'White', value: '#ffffff' },
  { label: 'Yellow', value: '#facc15' },
  { label: 'Cyan', value: '#67e8f9' },
];
