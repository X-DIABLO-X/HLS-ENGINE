'use client';

import { useEffect, useState, useRef } from 'react';
import Image from 'next/image';
import Link from 'next/link';
import {
  getVideos,
  deleteVideo,
  retryVideo,
  formatDuration,
  pollVideoStatus,
  pollVideoProgress,
} from '@/lib/api';
import { AuthGuard } from '@/components/auth/AuthGuard';
import {
  Video,
  VideoListResponse,
  ProcessingProgress,
} from '@/types/video';
import {
  Film,
  Loader2,
  AlertCircle,
  Clock,
  Loader,
  Trash2,
  X,
  RotateCcw,
} from 'lucide-react';

interface VideoPollHandles {
  stopProgress: () => void;
  stopStatus: () => void;
}

export default function VideosPage() {
  const [data, setData] = useState<VideoListResponse | null>(null);
  const [progress, setProgress] = useState<
    Record<string, ProcessingProgress>
  >({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<Video | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [retryingId, setRetryingId] = useState<string | null>(null);
  const pollRefs = useRef<Record<string, VideoPollHandles>>({});

  useEffect(() => {
    getVideos()
      .then((response) => {
        setData(response);
      })
      .catch((err) => {
        setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        setLoading(false);
      });
  }, []);

  useEffect(() => {
    const processingVideos = (data?.videos ?? []).filter(
      (v) => v.status === 'uploading' || v.status === 'processing'
    );
    const processingIds = new Set(processingVideos.map((video) => video.id));

    for (const [videoId, handles] of Object.entries(pollRefs.current)) {
      if (processingIds.has(videoId)) continue;
      handles.stopProgress();
      handles.stopStatus();
      delete pollRefs.current[videoId];
    }

    for (const video of processingVideos) {
      if (pollRefs.current[video.id]) continue;
      const stopProgress = pollVideoProgress(
        video.id,
        (p) => {
          setProgress((prev) => ({ ...prev, [video.id]: p }));
        }
      );
      const stopStatus = pollVideoStatus(video.id, (updated) => {
        setData((prev) => {
          if (!prev || !prev.videos) return prev;
          return {
            ...prev,
            videos: prev.videos.map((v) =>
              v.id === updated.id ? updated : v
            ),
          };
        });
      });

      pollRefs.current[video.id] = { stopProgress, stopStatus };
    }
  }, [data?.videos]);

  useEffect(
    () => () => {
      for (const handles of Object.values(pollRefs.current)) {
        handles.stopProgress();
        handles.stopStatus();
      }
      pollRefs.current = {};
    },
    []
  );

  return (
    <AuthGuard>
      <div>
        <div className="mb-8 flex items-end justify-between">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Video library</h1>
            <p className="mt-2 text-muted-foreground">
              Browse and play your transcoded videos.
            </p>
          </div>
          <Link
            href="/"
            className="rounded-lg bg-accent px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-accent/90"
          >
            Upload new
          </Link>
        </div>

        {loading && (
          <div className="flex flex-col items-center justify-center gap-3 py-20">
            <Loader2 className="h-8 w-8 animate-spin text-accent" />
            <p className="text-sm text-muted-foreground">Loading videos…</p>
          </div>
        )}

        {error && (
          <div className="rounded-xl border border-red-500/30 bg-red-500/10 p-6 text-center">
            <AlertCircle className="mx-auto h-8 w-8 text-red-400" />
            <p className="mt-2 text-sm font-medium text-red-400">
              Failed to load videos
            </p>
            <p className="text-sm text-muted-foreground">{error}</p>
          </div>
        )}

        {!loading && !error && data && (
          <>
            {(!data.videos || data.videos.length === 0) ? (
              <div className="rounded-xl border border-dashed border-border bg-muted py-20 text-center">
                <Film className="mx-auto h-10 w-10 text-muted-foreground" />
                <p className="mt-3 font-medium">No videos yet</p>
                <p className="text-sm text-muted-foreground">
                  Upload your first video to get started.
                </p>
              </div>
            ) : (
              <div className="grid gap-6 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
                {(data.videos || []).map((video) => (
                  <VideoCard
                    key={video.id}
                    video={video}
                    progress={progress[video.id]}
                    onDelete={() => setDeleteTarget(video)}
                    onRetry={async () => {
                      setRetryingId(video.id);
                      try {
                        await retryVideo(video.id);
                        // Update status to processing immediately
                        setData((prev) => {
                          if (!prev || !prev.videos) return prev;
                          return {
                            ...prev,
                            videos: prev.videos.map((v) =>
                              v.id === video.id
                                ? { ...v, status: 'processing' }
                                : v
                            ),
                          };
                        });
                      } catch {
                        // error shown via card state
                      } finally {
                        setRetryingId(null);
                      }
                    }}
                    retrying={retryingId === video.id}
                  />
                ))}
              </div>
            )}
          </>
        )}

        {deleteTarget && (
          <DeleteConfirmModal
            video={deleteTarget}
            loading={deleting}
            error={deleteError}
            onCancel={() => {
              setDeleteTarget(null);
              setDeleteError(null);
            }}
            onConfirm={async () => {
              setDeleting(true);
              setDeleteError(null);
              try {
                await deleteVideo(deleteTarget.id);
                setData((prev) => {
                  if (!prev || !prev.videos) return prev;
                  return {
                    ...prev,
                    videos: prev.videos.filter(
                      (v) => v.id !== deleteTarget.id
                    ),
                    total: prev.total - 1,
                  };
                });
                setDeleteTarget(null);
              } catch (err) {
                setDeleteError(
                  err instanceof Error ? err.message : String(err)
                );
              } finally {
                setDeleting(false);
              }
            }}
          />
        )}
      </div>
    </AuthGuard>
  );
}

function VideoCard({
  video,
  progress,
  onDelete,
  onRetry,
  retrying,
}: {
  video: Video;
  progress?: ProcessingProgress;
  onDelete: () => void;
  onRetry: () => void;
  retrying: boolean;
}) {
  const isProcessing =
    video.status === 'uploading' || video.status === 'processing';
  const percent = progress?.percent || 0;
  const stage = progress?.stage || video.status;
  const completedTasks = progress?.completed_tasks || 0;
  const totalTasks = progress?.total_tasks || 0;

  const statusColor =
    video.status === 'ready'
      ? 'bg-green-500/10 text-green-400'
      : video.status === 'failed'
      ? 'bg-red-500/10 text-red-400'
      : 'bg-yellow-500/10 text-yellow-400';

  const statusLabel =
    video.status === 'uploading'
      ? 'Uploading'
      : video.status === 'processing'
      ? 'Processing'
      : video.status === 'ready'
      ? 'Ready'
      : video.status === 'failed'
      ? 'Failed'
      : video.status;

  return (
    <Link
      href={`/videos/${video.id}`}
      className="group overflow-hidden rounded-xl border border-border bg-muted transition-colors hover:border-accent/50"
    >
      <div className="relative aspect-video bg-black">
        {video.thumbnailUrl ? (
          <Image
            src={video.thumbnailUrl}
            alt={video.title}
            width={640}
            height={360}
            unoptimized
            className="h-full w-full object-cover transition-transform duration-300 group-hover:scale-105"
            loading="lazy"
          />
        ) : (
          <div className="flex h-full items-center justify-center">
            <Film className="h-10 w-10 text-muted-foreground" />
          </div>
        )}
        {isProcessing && (
          <div className="absolute inset-0 flex flex-col items-center justify-center bg-black/70 p-4">
            <Loader className="h-8 w-8 animate-spin text-yellow-400" />
            <span className="mt-2 text-xs font-medium text-yellow-400">
              {stage}
            </span>
            {totalTasks > 0 && (
              <span className="text-[10px] text-muted-foreground">
                {completedTasks}/{totalTasks} tasks
              </span>
            )}
          </div>
        )}
        {video.duration !== undefined && video.duration > 0 && (
          <div className="absolute bottom-2 right-2 flex items-center gap-1 rounded bg-black/70 px-1.5 py-0.5 text-xs text-white">
            <Clock className="h-3 w-3" />
            {formatDuration(video.duration)}
          </div>
        )}
        <button
          type="button"
          onClick={(e) => {
            e.preventDefault();
            e.stopPropagation();
            onDelete();
          }}
          className="absolute right-2 top-2 z-10 flex h-8 w-8 items-center justify-center rounded-lg bg-black/70 text-white/80 opacity-0 transition-all hover:bg-red-500/80 hover:text-white focus:opacity-100 group-hover:opacity-100"
          aria-label="Delete video"
          title="Delete video"
        >
          <Trash2 className="h-4 w-4" />
        </button>
      </div>
      <div className="p-4">
        <h3 className="truncate font-medium">{video.title}</h3>
        {isProcessing && percent > 0 && (
          <div className="mt-2">
            <div className="mb-1 flex justify-between text-xs text-muted-foreground">
              <span>{percent}%</span>
            </div>
            <div className="h-1.5 w-full overflow-hidden rounded-full bg-background">
              <div
                className="h-full rounded-full bg-yellow-400 transition-all duration-300"
                style={{ width: `${percent}%` }}
              />
            </div>
          </div>
        )}
        <div className="mt-2 flex items-center justify-between">
          <span
            className={`rounded-full px-2 py-0.5 text-xs font-medium ${statusColor}`}
          >
            {statusLabel}
          </span>
          <span className="text-xs text-muted-foreground">
            {new Date(video.createdAt).toLocaleDateString()}
          </span>
        </div>
        {video.status === 'failed' && (
          <button
            type="button"
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              onRetry();
            }}
            disabled={retrying}
            className="mt-3 flex w-full items-center justify-center gap-1.5 rounded-lg border border-border px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent/10 hover:text-accent disabled:opacity-50"
          >
            {retrying ? (
              <Loader2 className="h-3 w-3 animate-spin" />
            ) : (
              <RotateCcw className="h-3 w-3" />
            )}
            Retry
          </button>
        )}
      </div>
    </Link>
  );
}

function DeleteConfirmModal({
  video,
  loading,
  error,
  onCancel,
  onConfirm,
}: {
  video: Video;
  loading: boolean;
  error: string | null;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
      onClick={onCancel}
    >
      <div
        className="w-full max-w-md rounded-xl border border-border bg-background p-6 shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-red-500/10">
              <Trash2 className="h-5 w-5 text-red-400" />
            </div>
            <div>
              <h2 className="text-lg font-semibold">Delete video</h2>
              <p className="text-sm text-muted-foreground">
                This action cannot be undone.
              </p>
            </div>
          </div>
          <button
            onClick={onCancel}
            className="rounded-lg p-1 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
            aria-label="Close"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <p className="mt-4 text-sm text-muted-foreground">
          Are you sure you want to delete{' '}
          <span className="font-medium text-foreground">{video.title}</span>?
          All transcoded renditions, audio tracks, and metadata will be
          permanently removed.
        </p>

        {error && (
          <div className="mt-3 rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">
            {error}
          </div>
        )}

        <div className="mt-6 flex justify-end gap-3">
          <button
            onClick={onCancel}
            disabled={loading}
            className="rounded-lg border border-border px-4 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            disabled={loading}
            className="flex items-center gap-2 rounded-lg bg-red-500 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-red-600 disabled:opacity-50"
          >
            {loading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Trash2 className="h-4 w-4" />
            )}
            Delete
          </button>
        </div>
      </div>
    </div>
  );
}
