export interface Rendition {
  id: string;
  name?: string;
  is_original?: boolean;
  /** Set for the matching HLS level after the manifest is parsed. */
  isOriginal?: boolean;
  width: number;
  height: number;
  bitrate: number;
  codec?: string;
  frameRate?: number;
}

export interface AudioTrack {
  id: string;
  lang: string;
  name: string;
  default?: boolean;
  autoselect?: boolean;
  forced?: boolean;
  delayMs?: number;
}

export interface Subtitle {
  id: string;
  lang: string;
  name: string;
  default?: boolean;
  forced?: boolean;
}

export interface Video {
  id: string;
  title: string;
  description?: string;
  status: 'uploading' | 'processing' | 'ready' | 'failed';
  thumbnailUrl?: string;
  duration?: number;
  createdAt: string;
  updatedAt?: string;
  manifestUrl?: string;
  renditions?: Rendition[];
  audioTracks?: AudioTrack[];
  subtitles?: Subtitle[];
}

export interface VideoListResponse {
  videos: Video[];
  total: number;
  page: number;
  pageSize: number;
}

export interface SignedUrlResponse {
  url: string;
  token?: string;
  expiresAt?: string;
}

export interface UploadProgress {
  loaded: number;
  total: number;
  percentage: number;
}

export interface TaskProgress {
  status: 'pending' | 'running' | 'completed' | 'failed';
  percent: number;
  error?: string;
}

export interface ProcessingProgress {
  percent: number;
  stage: string;
  total_tasks: number;
  completed_tasks: number;
  tasks: Record<string, TaskProgress>;
}

export interface TranscodingSettings {
  qualities: number[];
  audio_bitrate_kbps: number;
  audio_channels: number;
  segment_duration_sec: number;
  video_preset: string;
  force_cpu: boolean;
}

export const QUALITY_OPTIONS = [
  { value: 2160, label: '4K (2160p)' },
  { value: 1080, label: 'Full HD (1080p)' },
  { value: 720, label: 'HD (720p)' },
  { value: 480, label: 'SD (480p)' },
  { value: 360, label: 'Low (360p)' },
  { value: 240, label: 'Very Low (240p)' },
  { value: 144, label: 'Minimal (144p)' },
];

export const AUDIO_BITRATE_OPTIONS = [
  { value: 256, label: '256 kbps (High)' },
  { value: 192, label: '192 kbps (Good)' },
  { value: 128, label: '128 kbps (Standard)' },
  { value: 96, label: '96 kbps (Low)' },
  { value: 64, label: '64 kbps (Very Low)' },
];

export const AUDIO_CHANNEL_OPTIONS = [
  { value: 2, label: 'Stereo (2.0)' },
  { value: 6, label: 'Surround (5.1)' },
];

export const PRESET_OPTIONS = [
  { value: 'ultrafast', label: 'Ultra Fast' },
  { value: 'superfast', label: 'Super Fast' },
  { value: 'veryfast', label: 'Very Fast' },
  { value: 'faster', label: 'Faster' },
  { value: 'fast', label: 'Fast' },
  { value: 'medium', label: 'Medium (Recommended)' },
  { value: 'slow', label: 'Slow (Better Quality)' },
  { value: 'slower', label: 'Slower' },
];
