'use client';

import { useCallback, useState, useEffect } from 'react';
import Link from 'next/link';
import { useDropzone } from 'react-dropzone';
import { Upload, FileVideo, X, Loader2 } from 'lucide-react';
import { uploadFile, getTranscodingSettings } from '@/lib/api';
import { UploadProgress, TranscodingSettings } from '@/types/video';

interface UploadDropzoneProps {
  onUploadComplete?: (videoId: string) => void;
  onUploadError?: (error: Error) => void;
}

export function UploadDropzone({
  onUploadComplete,
  onUploadError,
}: UploadDropzoneProps) {
  const [file, setFile] = useState<File | null>(null);
  const [title, setTitle] = useState('');
  const [progress, setProgress] = useState<UploadProgress | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const [settings, setSettings] = useState<TranscodingSettings | null>(null);

  useEffect(() => {
    getTranscodingSettings().then(setSettings).catch(() => {});
  }, []);

  const onDrop = useCallback((acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      const selected = acceptedFiles[0];
      setFile(selected);
      if (!title) {
        setTitle(selected.name.replace(/\.[^/.]+$/, ''));
      }
    }
  }, [title]);

  const { getRootProps, getInputProps, isDragActive, fileRejections } =
    useDropzone({
      onDrop,
      accept: {
        'video/*': ['.mp4', '.mov', '.mkv', '.webm', '.avi', '.m4v'],
      },
      maxFiles: 1,
      disabled: isUploading,
    });

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!file) return;

    setIsUploading(true);
    setProgress(null);

    try {
      const video = await uploadFile(file, title || file.name, (p) => {
        setProgress(p);
      });
      onUploadComplete?.(video.id);
      setFile(null);
      setTitle('');
      setProgress(null);
    } catch (error) {
      onUploadError?.(error instanceof Error ? error : new Error(String(error)));
    } finally {
      setIsUploading(false);
    }
  };

  const clearFile = () => {
    setFile(null);
    setTitle('');
    setProgress(null);
  };

  return (
    <form onSubmit={handleSubmit} className="w-full">
      <div
        {...getRootProps()}
        className={`relative cursor-pointer rounded-2xl border-2 border-dashed p-8 transition-colors ${
          isDragActive
            ? 'border-accent bg-accent/10'
            : 'border-border bg-muted hover:border-white/30'
        } ${isUploading ? 'pointer-events-none opacity-70' : ''}`}
      >
        <input {...getInputProps()} />
        <div className="flex flex-col items-center justify-center gap-3 text-center">
          <div className="rounded-full bg-accent/10 p-4">
            <Upload className="h-8 w-8 text-accent" />
          </div>
          <div>
            <p className="text-lg font-medium">
              {isDragActive ? 'Drop the video here' : 'Drag & drop a video'}
            </p>
            <p className="text-sm text-muted-foreground">
              or click to browse files
            </p>
          </div>
          <p className="text-xs text-muted-foreground">
            Supports MP4, MOV, MKV, WEBM up to your gateway limit
          </p>
        </div>
      </div>

      {fileRejections.length > 0 && (
        <div className="mt-3 rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">
          Only video files are accepted.
        </div>
      )}

      {settings && (
        <div className="mt-4 rounded-xl border border-border bg-muted p-4">
          <p className="mb-2 text-xs font-medium text-muted-foreground">
            Active transcoding settings:
          </p>
          <div className="flex flex-wrap gap-2 text-xs">
            {settings.qualities.map((q) => (
              <span
                key={q}
                className="rounded-full bg-accent/10 px-2 py-1 font-medium text-accent"
              >
                {q}p
              </span>
            ))}
            <span className="rounded-full bg-background px-2 py-1 text-muted-foreground">
              {settings.audio_bitrate_kbps}kbps audio
            </span>
            <span className="rounded-full bg-background px-2 py-1 text-muted-foreground">
              {settings.segment_duration_sec}s segments
            </span>
            <Link
              href="/settings"
              className="rounded-full bg-background px-2 py-1 text-accent underline"
            >
              Edit settings
            </Link>
          </div>
        </div>
      )}

      {file && (
        <div className="mt-4 rounded-xl border border-border bg-muted p-4">
          <div className="flex items-start justify-between gap-3">
            <div className="flex items-center gap-3 overflow-hidden">
              <FileVideo className="h-8 w-8 shrink-0 text-accent" />
              <div className="min-w-0">
                <p className="truncate text-sm font-medium">{file.name}</p>
                <p className="text-xs text-muted-foreground">
                  {(file.size / (1024 * 1024)).toFixed(2)} MB
                </p>
              </div>
            </div>
            {!isUploading && (
              <button
                type="button"
                onClick={clearFile}
                className="rounded-full p-1 text-muted-foreground hover:bg-white/10 hover:text-white"
                aria-label="Remove file"
              >
                <X className="h-4 w-4" />
              </button>
            )}
          </div>

          <div className="mt-3">
            <label
              htmlFor="video-title"
              className="mb-1 block text-xs font-medium text-muted-foreground"
            >
              Title
            </label>
            <input
              id="video-title"
              type="text"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              placeholder="Enter video title"
              className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm outline-none placeholder:text-muted-foreground focus-visible:ring-2 focus-visible:ring-accent"
              disabled={isUploading}
            />
          </div>

          {progress && (
            <div className="mt-3">
              <div className="mb-1 flex justify-between text-xs text-muted-foreground">
                <span>Uploading…</span>
                <span>{progress.percentage}%</span>
              </div>
              <div className="h-2 w-full overflow-hidden rounded-full bg-background">
                <div
                  className="h-full rounded-full bg-accent transition-all duration-300"
                  style={{ width: `${progress.percentage}%` }}
                />
              </div>
            </div>
          )}

          <button
            type="submit"
            disabled={isUploading}
            className="mt-4 flex w-full items-center justify-center gap-2 rounded-lg bg-accent px-4 py-2.5 text-sm font-medium text-white transition-colors hover:bg-accent/90 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {isUploading ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" />
                Uploading…
              </>
            ) : (
              'Upload video'
            )}
          </button>
        </div>
      )}
    </form>
  );
}
