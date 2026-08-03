'use client';

import { useState } from 'react';
import Link from 'next/link';
import { UploadDropzone } from '@/components/upload/UploadDropzone';
import { AuthGuard } from '@/components/auth/AuthGuard';
import { CheckCircle, AlertCircle, ArrowRight } from 'lucide-react';

export default function HomePage() {
  const [uploadedId, setUploadedId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  return (
    <AuthGuard>
      <div className="mx-auto max-w-2xl">
        <div className="mb-8 text-center">
          <h1 className="text-3xl font-bold tracking-tight sm:text-4xl">
            Upload your video
          </h1>
          <p className="mt-2 text-muted-foreground">
            Upload a file and HLS Engine will transcode it into adaptive bitrate
            streams.
          </p>
        </div>

        {uploadedId && (
          <div className="mb-6 rounded-xl border border-green-500/30 bg-green-500/10 p-4">
            <div className="flex items-center gap-3">
              <CheckCircle className="h-5 w-5 text-green-400" />
              <p className="text-sm font-medium text-green-400">
                Upload complete
              </p>
            </div>
            <p className="mt-1 text-sm text-muted-foreground">
              Your video is being processed. You can view it in the library once
              ready.
            </p>
            <Link
              href={`/videos/${uploadedId}`}
              className="mt-3 inline-flex items-center gap-1 text-sm font-medium text-accent hover:underline"
            >
              Watch now <ArrowRight className="h-4 w-4" />
            </Link>
          </div>
        )}

        {error && (
          <div className="mb-6 rounded-xl border border-red-500/30 bg-red-500/10 p-4">
            <div className="flex items-center gap-3">
              <AlertCircle className="h-5 w-5 text-red-400" />
              <p className="text-sm font-medium text-red-400">Upload failed</p>
            </div>
            <p className="mt-1 text-sm text-muted-foreground">{error}</p>
          </div>
        )}

        <UploadDropzone
          onUploadComplete={(id) => {
            setUploadedId(id);
            setError(null);
          }}
          onUploadError={(err) => {
            setError(err.message);
            setUploadedId(null);
          }}
        />

        <div className="mt-8 grid gap-4 sm:grid-cols-3">
          {[
            'Drag & drop any video',
            'Adaptive bitrate transcoding',
            'Signed URL playback',
          ].map((text) => (
            <div
              key={text}
              className="rounded-xl border border-border bg-muted p-4 text-center text-sm text-muted-foreground"
            >
              {text}
            </div>
          ))}
        </div>
      </div>
    </AuthGuard>
  );
}
