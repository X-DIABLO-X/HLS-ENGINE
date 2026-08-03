'use client';

import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
import { Player } from '@/components/player/Player';
import { getEmbedManifest, EmbedManifestResponse } from '@/lib/api';
import { Loader2, AlertCircle } from 'lucide-react';

/**
 * Public embeddable player page.
 *
 * This page is intentionally NOT wrapped in AuthGuard so it can be loaded
 * inside an <iframe> on third-party sites. The signed manifest URL returned
 * by the public /api/v1/videos/{id}/embed endpoint is the only credential
 * needed to stream the HLS content.
 *
 * Embed code example:
 *   <iframe
 *     src="https://your-host/embed/VIDEO_ID"
 *     width="1280" height="720"
 *     frameborder="0"
 *     allow="autoplay; fullscreen; encrypted-media"
 *     allowfullscreen
 *   ></iframe>
 */
export default function EmbedPage() {
  const params = useParams();
  const id = typeof params.id === 'string' ? params.id : '';

  return <EmbedPageContent key={id} id={id} />;
}

function EmbedPageContent({ id }: { id: string }) {
  const [embed, setEmbed] = useState<EmbedManifestResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;

    let cancelled = false;

    getEmbedManifest(id)
      .then((data) => {
        if (cancelled) return;
        setEmbed(data);
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

  if (loading) {
    return (
      <div className="flex h-screen w-screen items-center justify-center bg-black">
        <Loader2 className="h-8 w-8 animate-spin text-white" />
      </div>
    );
  }

  if (error || !embed) {
    return (
      <div className="flex h-screen w-screen flex-col items-center justify-center gap-3 bg-black p-6 text-center">
        <AlertCircle className="h-8 w-8 text-red-400" />
        <p className="text-sm font-medium text-red-400">
          {error === 'video not ready'
            ? 'This video is still being processed.'
            : error === 'video not found'
            ? 'Video not found.'
            : 'Unable to load video.'}
        </p>
      </div>
    );
  }

  return (
    <div className="h-screen w-screen overflow-hidden bg-black">
      <Player
        manifestUrl={
          process.env.NEXT_PUBLIC_HLS_BASE_URL
            ? new URL(embed.url, process.env.NEXT_PUBLIC_HLS_BASE_URL).href
            : embed.url
        }
        title={embed.title}
        onError={(err) => setError(err.message)}
      />
    </div>
  );
}
