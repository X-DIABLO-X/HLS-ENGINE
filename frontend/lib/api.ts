import axios, { AxiosProgressEvent, InternalAxiosRequestConfig } from 'axios';
import {
  SignedUrlResponse,
  UploadProgress,
  Video,
  VideoListResponse,
  ProcessingProgress,
  TranscodingSettings,
} from '@/types/video';
import { useAuthStore } from '@/lib/auth';

const api = axios.create({
  baseURL: '/api/v1',
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 60000,
});

api.interceptors.request.use((config) => {
  const token = useAuthStore.getState().getToken();
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

type RetryableRequest = InternalAxiosRequestConfig & { _retried?: boolean };

// Single-flight token refresh so concurrent 401s share one refresh attempt.
let refreshPromise: Promise<boolean> | null = null;

api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const status = error.response?.status;
    const original = error.config as RetryableRequest | undefined;

    if (status === 401 && original && !original._retried) {
      original._retried = true;
      if (!refreshPromise) {
        refreshPromise = useAuthStore.getState().refresh().finally(() => {
          refreshPromise = null;
        });
      }
      const refreshed = await refreshPromise;
      if (refreshed) {
        const newToken = useAuthStore.getState().getToken();
        original.headers = original.headers ?? {};
        original.headers.Authorization = `Bearer ${newToken}`;
        return api(original);
      }
      if (typeof window !== 'undefined' && window.location.pathname !== '/login') {
        window.location.href = '/login';
      }
      return Promise.reject(new Error('Session expired'));
    }

    const message =
      error.response?.data?.message || error.response?.data?.error || error.message || 'Unknown error';
    return Promise.reject(new Error(message));
  }
);

export async function getVideos(
  page = 1,
  pageSize = 24
): Promise<VideoListResponse> {
  const { data } = await api.get<VideoListResponse>('/videos', {
    params: { page, pageSize },
  });
  return data;
}

export async function getVideo(id: string): Promise<Video> {
  const { data } = await api.get<Video>(`/videos/${id}`);
  return data;
}

export async function getSignedManifest(
  videoId: string
): Promise<SignedUrlResponse> {
  return withRetries(
    async () => {
      const { data } = await api.get<SignedUrlResponse>(
        `/videos/${videoId}/manifest`,
        { timeout: 15000 }
      );
      return data;
    },
    4
  );
}

export async function createVideo(title: string, description?: string): Promise<Video> {
  const { data } = await api.post<Video>('/videos', { title, description });
  return data;
}

export interface MultipartUploadSessionResponse {
  session_id: string;
  video_id: string;
  upload_id: string;
  object_name: string;
  filename: string;
  content_type: string;
  size: number;
  part_size: number;
  status: string;
  expires_in: number;
  parts?: number[];
}

async function delay(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function withRetries<T>(fn: () => Promise<T>, attempts = 3): Promise<T> {
  let lastError: unknown;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (attempt < attempts - 1) {
        await delay(500 * 2 ** attempt);
      }
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

const MAX_CONSECUTIVE_POLL_ERRORS = 5;
const MAX_POLL_RETRY_DELAY_MS = 15000;
// Browsers typically allow six HTTP/1.1 connections per origin. Use that
// capacity for independent multipart PUTs instead of paying one round trip per
// chunk serially. The server streams each part, so memory remains bounded.
const MAX_PARALLEL_UPLOADS = 6;

function pollRetryDelay(consecutiveErrors: number): number {
  return Math.min(
    1000 * 2 ** Math.max(0, consecutiveErrors - 1),
    MAX_POLL_RETRY_DELAY_MS
  );
}

async function createMultipartUploadSession(
  videoId: string,
  file: File
): Promise<MultipartUploadSessionResponse> {
  const { data } = await api.post<MultipartUploadSessionResponse>(
    `/videos/${videoId}/upload/session`,
    {
      filename: file.name,
      size: file.size,
      content_type: file.type || 'application/octet-stream',
    }
  );
  return data;
}

async function uploadMultipartPart(
  videoId: string,
  sessionId: string,
  partNumber: number,
  chunk: Blob,
  onProgress?: (loaded: number) => void
): Promise<void> {
  await api.put(
    `/videos/${videoId}/upload/session/${sessionId}/parts/${partNumber}`,
    chunk,
    {
      headers: {
        'Content-Type': 'application/octet-stream',
      },
      timeout: 0,
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
      onUploadProgress: (event: AxiosProgressEvent) => {
        if (event.loaded !== undefined && onProgress) {
          onProgress(event.loaded);
        }
      },
    }
  );
}

async function completeMultipartUploadSession(
  videoId: string,
  sessionId: string
): Promise<void> {
  await api.post(
    `/videos/${videoId}/upload/session/${sessionId}/complete`
  );
}

async function abortMultipartUploadSession(
  videoId: string,
  sessionId: string
): Promise<void> {
  await api.post(`/videos/${videoId}/upload/session/${sessionId}/abort`);
}

export async function uploadFile(
  file: File,
  title?: string,
  onProgress?: (progress: UploadProgress) => void
): Promise<Video> {
  // 1. Create the video metadata record first so we have an ID.
  const video = await createVideo(title || file.name.replace(/\.[^/.]+$/, ''), '');

  // 2. Create a resumable multipart session and upload the file in chunks.
  const session = await createMultipartUploadSession(video.id, file);
  const partSize = session.part_size || 32 * 1024 * 1024;
  const totalParts = Math.max(1, Math.ceil(file.size / partSize));
  const loadedByPart = new Array<number>(totalParts).fill(0);
  let nextPartIndex = 0;
  let uploadError: unknown;

  const reportAggregateProgress = () => {
    if (!onProgress) return;
    const loaded = Math.min(
      loadedByPart.reduce((total, partLoaded) => total + partLoaded, 0),
      file.size
    );
    onProgress({
      loaded,
      total: file.size,
      percentage: Math.round((loaded * 100) / file.size),
    });
  };

  try {
    const uploadWorker = async () => {
      while (uploadError === undefined) {
        const partIndex = nextPartIndex;
        nextPartIndex += 1;
        if (partIndex >= totalParts) return;

        const partNumber = partIndex + 1;
        const start = partIndex * partSize;
        const end = Math.min(start + partSize, file.size);
        const chunk = file.slice(start, end);

        try {
          await withRetries(async () => {
            // A retry starts this part from byte zero. Reset its contribution
            // so aggregate progress never double-counts a previous attempt.
            loadedByPart[partIndex] = 0;
            reportAggregateProgress();
            await uploadMultipartPart(
              video.id,
              session.session_id,
              partNumber,
              chunk,
              (partLoaded) => {
                loadedByPart[partIndex] = Math.min(partLoaded, chunk.size);
                reportAggregateProgress();
              }
            );
          }, 3);
          loadedByPart[partIndex] = chunk.size;
          reportAggregateProgress();
        } catch (error) {
          uploadError = error;
          return;
        }
      }
    };

    const workerCount = Math.min(MAX_PARALLEL_UPLOADS, totalParts);
    await Promise.all(
      Array.from({ length: workerCount }, () => uploadWorker())
    );
    if (uploadError !== undefined) {
      throw uploadError;
    }

    await completeMultipartUploadSession(video.id, session.session_id);
    if (onProgress) {
      onProgress({ loaded: file.size, total: file.size, percentage: 100 });
    }
    return video;
  } catch (error) {
    try {
      await abortMultipartUploadSession(video.id, session.session_id);
    } catch {
      // Best effort cleanup.
    }
    throw error;
  }
}

export async function deleteVideo(id: string): Promise<void> {
  await api.delete(`/videos/${id}`);
}

export interface RetryJobResponse {
  job_id: string;
  video_id: string;
  status: string;
  message: string;
}

export async function retryVideo(videoId: string): Promise<RetryJobResponse> {
  const { data } = await api.post<RetryJobResponse>(
    `/videos/${videoId}/retry`
  );
  return data;
}

/**
 * Public (unauthenticated) embed manifest fetch.
 *
 * The embed endpoint returns a signed HLS URL + basic video metadata so the
 * /embed/[id] page can render the player without a user session. The signed
 * token embedded in the URL is the only credential needed to fetch segments.
 */
export interface EmbedManifestResponse {
  id: string;
  title: string;
  status: string;
  duration?: number;
  url: string;
  token?: string;
  expiresAt?: string;
}

const publicApi = axios.create({
  baseURL: '/api/v1',
  headers: { 'Content-Type': 'application/json' },
  timeout: 15000,
});

export async function getEmbedManifest(videoId: string): Promise<EmbedManifestResponse> {
  const { data } = await publicApi.get<EmbedManifestResponse>(
    `/videos/${videoId}/embed`
  );
  return data;
}

export function pollVideoStatus(
  id: string,
  onUpdate: (video: Video) => void,
  isTerminal: (status: string) => boolean = (s) => s === 'ready' || s === 'failed'
): () => void {
  let cancelled = false;
  let cancelWait: (() => void) | null = null;

  const wait = (ms: number): Promise<void> =>
    new Promise((resolve) => {
      if (cancelled) {
        resolve();
        return;
      }

      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        cancelWait = null;
        resolve();
      };
      const timer = setTimeout(finish, ms);
      cancelWait = () => {
        clearTimeout(timer);
        finish();
      };
    });

  const poll = async () => {
    let consecutiveErrors = 0;
    let nextDelay = 5000;

    while (!cancelled) {
      await wait(nextDelay);
      if (cancelled) break;

      try {
        const video = await getVideo(id);
        if (cancelled) break;

        consecutiveErrors = 0;
        nextDelay = 5000;
        onUpdate(video);
        if (isTerminal(video.status)) break;
      } catch {
        consecutiveErrors += 1;
        if (consecutiveErrors >= MAX_CONSECUTIVE_POLL_ERRORS) break;
        nextDelay = pollRetryDelay(consecutiveErrors);
      }
    }
  };
  void poll();

  return () => {
    cancelled = true;
    cancelWait?.();
  };
}

export async function getVideoProgress(videoId: string): Promise<ProcessingProgress> {
  const { data } = await api.get<ProcessingProgress>(
    `/videos/${videoId}/progress`
  );
  return data;
}

export function pollVideoProgress(
  id: string,
  onUpdate: (progress: ProcessingProgress) => void,
  isTerminal: (percent: number, stage: string) => boolean = (p, s) =>
    p >= 100 || s === 'Failed' || s === 'Ready'
): () => void {
  let cancelled = false;
  let cancelWait: (() => void) | null = null;

  const wait = (ms: number): Promise<void> =>
    new Promise((resolve) => {
      if (cancelled) {
        resolve();
        return;
      }

      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        cancelWait = null;
        resolve();
      };
      const timer = setTimeout(finish, ms);
      cancelWait = () => {
        clearTimeout(timer);
        finish();
      };
    });

  const poll = async () => {
    let consecutiveErrors = 0;
    let nextDelay = 0;

    while (!cancelled) {
      await wait(nextDelay);
      if (cancelled) break;

      try {
        const progress = await getVideoProgress(id);
        if (cancelled) break;

        consecutiveErrors = 0;
        nextDelay = 3000;
        onUpdate(progress);
        if (isTerminal(progress.percent, progress.stage)) break;
      } catch {
        consecutiveErrors += 1;
        if (consecutiveErrors >= MAX_CONSECUTIVE_POLL_ERRORS) break;
        nextDelay = pollRetryDelay(consecutiveErrors);
      }
    }
  };
  void poll();

  return () => {
    cancelled = true;
    cancelWait?.();
  };
}

export async function getTranscodingSettings(): Promise<TranscodingSettings> {
  const { data } = await api.get<TranscodingSettings>('/transcoding-settings');
  return data;
}

export async function updateTranscodingSettings(
  settings: TranscodingSettings
): Promise<TranscodingSettings> {
  const { data } = await api.put<TranscodingSettings>(
    '/transcoding-settings',
    settings
  );
  return data;
}

export function formatDuration(seconds = 0): string {
  const hrs = Math.floor(seconds / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);

  if (hrs > 0) {
    return `${hrs}:${pad(mins)}:${pad(secs)}`;
  }
  return `${pad(mins)}:${pad(secs)}`;
}

function pad(num: number): string {
  return num.toString().padStart(2, '0');
}
