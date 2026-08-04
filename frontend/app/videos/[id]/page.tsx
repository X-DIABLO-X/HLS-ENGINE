'use client';

import { useEffect, useMemo, useState, useRef } from 'react';
import Link from 'next/link';
import { useParams } from 'next/navigation';
import { Player } from '@/components/player/Player';
import { AuthGuard } from '@/components/auth/AuthGuard';
import {
  getVideo,
  getRenditions,
  getSignedManifest,
  deleteVideo,
  retryVideo,
  pollVideoStatus,
  pollVideoProgress,
  updateVideoShare,
} from '@/lib/api';
import { resolveManifestUrl } from '@/lib/manifest-url.mjs';
import { Video, SignedUrlResponse, ProcessingProgress } from '@/types/video';
import {
  Loader2,
  AlertCircle,
  ArrowLeft,
  Film,
  Loader,
  CheckCircle,
  Trash2,
  Code,
  Copy,
  Check,
  X,
  RotateCcw,
} from 'lucide-react';

export default function VideoPlayerPage() {
  const params = useParams();
  const id = typeof params.id === 'string' ? params.id : '';

  return <VideoPlayerPageContent key={id} id={id} />;
}

function VideoPlayerPageContent({ id }: { id: string }) {
  const [video, setVideo] = useState<Video | null>(null);
  const [progress, setProgress] = useState<ProcessingProgress | null>(null);
  const [signedManifest, setSignedManifest] =
    useState<SignedUrlResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showEmbed, setShowEmbed] = useState(false);
  const [copied, setCopied] = useState(false);
  const [deleteConfirm, setDeleteConfirm] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [retrying, setRetrying] = useState(false);
  const [retryError, setRetryError] = useState<string | null>(null);
  const progressPollRef = useRef<(() => void) | null>(null);

  useEffect(() => {
    if (!id) return;

    let cancelled = false;

    getVideo(id)
      .then(async (videoData) => {
        if (cancelled) return;
        try {
          const renditions = await getRenditions(id);
          if (cancelled) return;
          setVideo({ ...videoData, renditions });
        } catch {
          // Renditions are enhancement metadata for the selector. Keep the
          // player available if an older video has no rendition records yet.
          if (cancelled) return;
          setVideo(videoData);
        }
      })
      .catch((err) => {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        if (cancelled) return;
        setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [id]);

  const videoStatus = video?.status;
  const originalResolution = useMemo(() => {
    const original = video?.renditions?.find(
      (rendition) => rendition.is_original
    );
    return original
      ? { width: original.width, height: original.height }
      : undefined;
  }, [video?.renditions]);

  useEffect(() => {
    if (!id || videoStatus !== 'ready' || signedManifest) return;

    let cancelled = false;

    getSignedManifest(id)
      .then((manifestData) => {
        if (cancelled) return;
        setSignedManifest(manifestData);
      })
      .catch((err) => {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : String(err));
      });

    return () => {
      cancelled = true;
    };
  }, [id, signedManifest, videoStatus]);

  useEffect(() => {
    if (
      !videoStatus ||
      videoStatus === 'ready' ||
      videoStatus === 'failed'
    ) {
      progressPollRef.current?.();
      return;
    }

    progressPollRef.current = pollVideoProgress(id, (p) => {
      setProgress(p);
    });

    const statusPoll = pollVideoStatus(
      id,
      (updated) => {
        setVideo(updated);
      }
    );

    return () => {
      progressPollRef.current?.();
      statusPoll();
    };
  }, [id, videoStatus]);

  const isProcessing =
    video && (video.status === 'uploading' || video.status === 'processing');
  const pageLoading =
    loading || (videoStatus === 'ready' && !signedManifest && !error);

  return (
    <AuthGuard>
      <div>
        <Link
          href="/videos"
          className="mb-4 inline-flex items-center gap-1 text-sm font-medium text-muted-foreground transition-colors hover:text-foreground"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to library
        </Link>

        {pageLoading && (
          <div className="flex flex-col items-center justify-center gap-3 py-20">
            <Loader2 className="h-8 w-8 animate-spin text-accent" />
            <p className="text-sm text-muted-foreground">Loading video…</p>
          </div>
        )}

        {error && (
          <div className="rounded-xl border border-red-500/30 bg-red-500/10 p-6 text-center">
            <AlertCircle className="mx-auto h-8 w-8 text-red-400" />
            <p className="mt-2 text-sm font-medium text-red-400">
              Failed to load video
            </p>
            <p className="text-sm text-muted-foreground">{error}</p>
          </div>
        )}

        {!pageLoading && !error && video && isProcessing && (
          <ProcessingView
            video={video}
            progress={progress}
            onRetry={async () => {
              setRetrying(true);
              setRetryError(null);
              try {
                await retryVideo(video.id);
                window.location.reload();
              } catch (err) {
                setRetryError(
                  err instanceof Error ? err.message : String(err)
                );
              } finally {
                setRetrying(false);
              }
            }}
            retrying={retrying}
          />
        )}

        {!pageLoading && !error && video && video.status === 'failed' && (
          <div className="rounded-xl border border-red-500/30 bg-red-500/10 p-6 text-center">
            <AlertCircle className="mx-auto h-8 w-8 text-red-400" />
            <p className="mt-2 text-sm font-medium text-red-400">
              Video processing failed
            </p>
            <p className="text-sm text-muted-foreground">
              {video.title} could not be transcoded. Please try uploading again.
            </p>
            {retryError && (
              <p className="mt-2 text-sm text-red-400">{retryError}</p>
            )}
            <button
              onClick={async () => {
                setRetrying(true);
                setRetryError(null);
                try {
                  await retryVideo(video.id);
                  window.location.reload();
                } catch (err) {
                  setRetryError(
                    err instanceof Error ? err.message : String(err)
                  );
                } finally {
                  setRetrying(false);
                }
              }}
              disabled={retrying}
              className="mt-4 inline-flex items-center gap-2 rounded-lg bg-accent px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-accent/90 disabled:opacity-50"
            >
              {retrying ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <RotateCcw className="h-4 w-4" />
              )}
              Retry processing
            </button>
          </div>
        )}

        {!pageLoading && !error && video && signedManifest && video.status === 'ready' && (
          <div className="space-y-6">
            <Player
              manifestUrl={resolveManifestUrl(signedManifest.url)}
              originalResolution={originalResolution}
              title={video.title}
              poster={video.thumbnailUrl}
              onError={(err) => setError(err.message)}
            />
            <div>
              <div className="flex items-start justify-between gap-4">
                <div className="flex items-start gap-4">
                  <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-muted">
                    <Film className="h-6 w-6 text-accent" />
                  </div>
                  <div>
                    <h1 className="text-2xl font-bold tracking-tight">
                      {video.title}
                    </h1>
                    {video.description && (
                      <p className="mt-1 text-muted-foreground">
                        {video.description}
                      </p>
                    )}
                    <div className="mt-2 flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
                      <span className="rounded-full bg-green-500/10 px-2 py-1 font-medium text-green-400">
                        Ready
                      </span>
                      <span>
                        Added {new Date(video.createdAt).toLocaleString()}
                      </span>
                    </div>
                  </div>
                </div>
                <div className="flex shrink-0 items-center gap-2">
                  <button
                    onClick={() => setShowEmbed((s) => !s)}
                    className="flex items-center gap-1.5 rounded-lg border border-border px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
                  >
                    <Code className="h-4 w-4" />
                    Embed
                  </button>
                  <button
                    onClick={() => setDeleteConfirm(true)}
                    className="flex items-center gap-1.5 rounded-lg border border-red-500/30 px-3 py-2 text-sm font-medium text-red-400 transition-colors hover:bg-red-500/10"
                  >
                    <Trash2 className="h-4 w-4" />
                    Delete
                  </button>
                </div>
              </div>

              {showEmbed && (
                <EmbedPanel
                  videoId={video.id}
                  title={video.title}
                  shareId={video.shareId}
                  shareEnabled={video.shareEnabled}
                  copied={copied}
                  onShareChanged={(updated) => setVideo((current) => current ? { ...current, shareId: updated.shareId, shareEnabled: updated.enabled } : current)}
                  onCopied={() => {
                    setCopied(true);
                    setTimeout(() => setCopied(false), 2000);
                  }}
                  onClose={() => setShowEmbed(false)}
                />
              )}
            </div>
          </div>
        )}

        {deleteConfirm && video && (
          <DeleteConfirmModal
            video={video}
            loading={deleting}
            error={deleteError}
            onCancel={() => {
              setDeleteConfirm(false);
              setDeleteError(null);
            }}
            onConfirm={async () => {
              setDeleting(true);
              setDeleteError(null);
              try {
                await deleteVideo(video.id);
                window.location.href = '/videos';
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

function EmbedPanel({
  videoId,
  title,
  shareId,
  shareEnabled,
  copied,
  onCopied,
  onShareChanged,
  onClose,
}: {
  videoId: string;
  title: string;
  shareId?: string;
  shareEnabled?: boolean;
  copied: boolean;
  onCopied: () => void;
  onShareChanged: (share: { shareId?: string; enabled: boolean }) => void;
  onClose: () => void;
}) {
  const [sharing, setSharing] = useState(false);
  const [shareError, setShareError] = useState<string | null>(null);
  const origin =
    typeof window !== 'undefined' ? window.location.origin : '';
  const embedUrl = shareId ? `${origin}/embed/${shareId}` : '';
  const aspect = 16 / 9;
  const embedCode = `<iframe
  src="${embedUrl}"
  width="1280"
  height="720"
  frameborder="0"
  allow="autoplay; fullscreen; encrypted-media"
  allowfullscreen
  title="${title.replace(/"/g, '&quot;')}"
></iframe>`;

  const copy = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      onCopied();
    } catch {
      // Fallback for browsers without clipboard API
      const ta = document.createElement('textarea');
      ta.value = text;
      document.body.appendChild(ta);
      ta.select();
      try {
        document.execCommand('copy');
        onCopied();
      } catch {
        /* ignore */
      }
      document.body.removeChild(ta);
    }
  };

  const updateShare = async (enabled: boolean, rotate = false) => {
    setSharing(true);
    setShareError(null);
    try {
      onShareChanged(await updateVideoShare(videoId, enabled, rotate));
    } catch (err) {
      setShareError(err instanceof Error ? err.message : String(err));
    } finally {
      setSharing(false);
    }
  };

  return (
    <div className="mt-4 rounded-xl border border-border bg-muted p-5">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold">Embed this video</h3>
        <button
          onClick={onClose}
          className="rounded-lg p-1 text-muted-foreground transition-colors hover:bg-background hover:text-foreground"
          aria-label="Close"
        >
          <X className="h-4 w-4" />
        </button>
      </div>
      <p className="mt-1 text-xs text-muted-foreground">
        Only an active, opaque share link can be embedded. You can disable or
        rotate it at any time; either action immediately revokes the old URL.
      </p>

      <div className="mt-3 flex flex-wrap items-center gap-2">
        {!shareEnabled || !shareId ? (
          <button onClick={() => updateShare(true)} disabled={sharing} className="rounded-lg bg-accent px-3 py-2 text-xs font-semibold text-white disabled:opacity-50">
            {sharing ? 'Creating link…' : 'Create share link'}
          </button>
        ) : (
          <>
            <button onClick={() => updateShare(true, true)} disabled={sharing} className="rounded-lg border border-border px-3 py-2 text-xs font-semibold text-foreground disabled:opacity-50">
              {sharing ? 'Updating…' : 'Rotate link'}
            </button>
            <button onClick={() => updateShare(false)} disabled={sharing} className="rounded-lg border border-red-500/30 px-3 py-2 text-xs font-semibold text-red-400 disabled:opacity-50">
              Disable link
            </button>
          </>
        )}
      </div>
      {shareError && <p className="mt-2 text-xs text-red-400">{shareError}</p>}

      {shareEnabled && shareId && <div className="mt-3 space-y-3">
        <div>
          <label className="text-xs font-medium text-muted-foreground">
            Embed code
          </label>
          <div className="relative mt-1">
            <pre className="overflow-x-auto rounded-lg border border-border bg-background p-3 pr-24 text-xs leading-relaxed">
              <code>{embedCode}</code>
            </pre>
            <button
              onClick={() => copy(embedCode)}
              className="absolute right-2 top-2 flex items-center gap-1 rounded-md bg-accent px-2 py-1 text-xs font-medium text-white transition-colors hover:bg-accent/90"
            >
              {copied ? (
                <>
                  <Check className="h-3 w-3" /> Copied
                </>
              ) : (
                <>
                  <Copy className="h-3 w-3" /> Copy
                </>
              )}
            </button>
          </div>
        </div>

        <div>
          <label className="text-xs font-medium text-muted-foreground">
            Embed URL
          </label>
          <div className="mt-1 flex items-center gap-2">
            <input
              readOnly
              value={embedUrl}
              className="flex-1 rounded-lg border border-border bg-background px-3 py-2 text-xs text-foreground outline-none"
              onClick={(e) => (e.target as HTMLInputElement).select()}
            />
            <button
              onClick={() => copy(embedUrl)}
              className="flex items-center gap-1 rounded-lg border border-border px-3 py-2 text-xs font-medium text-muted-foreground transition-colors hover:bg-background hover:text-foreground"
            >
              {copied ? (
                <>
                  <Check className="h-3 w-3" /> Copied
                </>
              ) : (
                <>
                  <Copy className="h-3 w-3" /> Copy
                </>
              )}
            </button>
          </div>
        </div>

        <div>
          <label className="text-xs font-medium text-muted-foreground">
            Responsive embed (16:9)
          </label>
          <div className="relative mt-1">
            <pre className="overflow-x-auto rounded-lg border border-border bg-background p-3 pr-24 text-xs leading-relaxed">
              <code>{`<div style="position:relative;padding-bottom:${(100 / aspect).toFixed(4)}%;height:0;">
  <iframe src="${embedUrl}"
    style="position:absolute;top:0;left:0;width:100%;height:100%;"
    frameborder="0" allow="autoplay; fullscreen; encrypted-media"
    allowfullscreen title="${title.replace(/"/g, '&quot;')}"></iframe>
</div>`}</code>
            </pre>
            <button
              onClick={() =>
                copy(
                  `<div style="position:relative;padding-bottom:${(100 / aspect).toFixed(4)}%;height:0;">
  <iframe src="${embedUrl}"
    style="position:absolute;top:0;left:0;width:100%;height:100%;"
    frameborder="0" allow="autoplay; fullscreen; encrypted-media"
    allowfullscreen title="${title.replace(/"/g, '&quot;')}"></iframe>
</div>`
                )
              }
              className="absolute right-2 top-2 flex items-center gap-1 rounded-md bg-accent px-2 py-1 text-xs font-medium text-white transition-colors hover:bg-accent/90"
            >
              {copied ? (
                <>
                  <Check className="h-3 w-3" /> Copied
                </>
              ) : (
                <>
                  <Copy className="h-3 w-3" /> Copy
                </>
              )}
            </button>
          </div>
        </div>
      </div>}
    </div>
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

function ProcessingView({
  video,
  progress,
  onRetry,
  retrying,
}: {
  video: Video;
  progress: ProcessingProgress | null;
  onRetry: () => void;
  retrying: boolean;
}) {
  const percent = progress?.percent || 0;
  const stage = progress?.stage || 'Initializing';
  const tasks = progress?.tasks || {};
  const completed = progress?.completed_tasks || 0;
  const total = progress?.total_tasks || 0;
  const looksStuck = percent >= 99 && stage.toLowerCase().includes('transcod');

  return (
    <div className="space-y-6">
      <div className="flex aspect-video w-full flex-col items-center justify-center rounded-xl border border-border bg-black p-8">
        <Loader className="h-12 w-12 animate-spin text-yellow-400" />
        <p className="mt-4 text-lg font-medium text-yellow-400">{stage}</p>
        <p className="mt-1 text-sm text-muted-foreground">
          Your video is being transcoded into adaptive bitrate streams.
        </p>

        {/* Overall progress bar */}
        <div className="mt-6 w-full max-w-md">
          <div className="mb-2 flex justify-between text-xs text-muted-foreground">
            <span>Overall Progress</span>
            <span>
              {percent}% ({completed}/{total} tasks)
            </span>
          </div>
          <div className="h-3 w-full overflow-hidden rounded-full bg-background">
            <div
              className="h-full rounded-full bg-yellow-400 transition-all duration-500"
              style={{ width: `${percent}%` }}
            />
          </div>
        </div>

        {looksStuck && (
          <div className="mt-6 max-w-md rounded-lg border border-yellow-500/30 bg-yellow-500/10 p-4 text-center">
            <p className="text-sm font-medium text-yellow-400">
              Processing appears to be stuck
            </p>
            <p className="mt-1 text-xs text-muted-foreground">
              If this doesn&apos;t complete soon, you can retry the transcoding
              job.
            </p>
            <button
              onClick={onRetry}
              disabled={retrying}
              className="mt-3 inline-flex items-center gap-2 rounded-lg bg-accent px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-accent/90 disabled:opacity-50"
            >
              {retrying ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <RotateCcw className="h-4 w-4" />
              )}
              Retry processing
            </button>
          </div>
        )}
      </div>

      {/* Task breakdown */}
      {Object.keys(tasks).length > 0 && (
        <div className="rounded-xl border border-border bg-muted p-6">
          <h2 className="mb-4 text-lg font-semibold">Processing Tasks</h2>
          <div className="space-y-2">
            {Object.entries(tasks).map(([name, task]) => (
              <div
                key={name}
                className="flex items-center gap-3 rounded-lg border border-border bg-background p-3"
              >
                <div className="shrink-0">
                  {task.status === 'completed' ? (
                    <CheckCircle className="h-5 w-5 text-green-400" />
                  ) : task.status === 'running' ? (
                    <Loader className="h-5 w-5 animate-spin text-yellow-400" />
                  ) : task.status === 'failed' ? (
                    <AlertCircle className="h-5 w-5 text-red-400" />
                  ) : (
                    <div className="h-5 w-5 rounded-full border-2 border-muted-foreground" />
                  )}
                </div>
                <div className="flex-1">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium capitalize">
                      {name.replace(/_/g, ' ')}
                    </span>
                    {task.status === 'running' && (
                      <span className="text-xs text-muted-foreground">
                        {task.percent}%
                      </span>
                    )}
                  </div>
                  {task.status === 'running' && (
                    <div className="mt-1 h-1 w-full overflow-hidden rounded-full bg-background">
                      <div
                        className="h-full rounded-full bg-yellow-400 transition-all duration-300"
                        style={{ width: `${task.percent}%` }}
                      />
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div>
        <div className="flex items-start gap-4">
          <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-muted">
            <Film className="h-6 w-6 text-accent" />
          </div>
          <div>
            <h1 className="text-2xl font-bold tracking-tight">{video.title}</h1>
            <div className="mt-2 flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
              <span className="rounded-full bg-yellow-500/10 px-2 py-1 font-medium text-yellow-400">
                Processing
              </span>
              <span>Added {new Date(video.createdAt).toLocaleString()}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
