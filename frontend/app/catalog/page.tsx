'use client';

import Link from 'next/link';
import { FormEvent, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { AuthGuard } from '@/components/auth/AuthGuard';
import { createCatalogTitle, createPlayable, getCatalogTitles, getVideos } from '@/lib/api';
import { CatalogTitle, Video } from '@/types/video';
import { Clapperboard, Film, Plus, Tv } from 'lucide-react';

type CreationKind = 'movie' | 'series' | null;

export default function CatalogPage() {
  return <AuthGuard><CatalogStudio /></AuthGuard>;
}

function CatalogStudio() {
  const router = useRouter();
  const [titles, setTitles] = useState<CatalogTitle[]>([]);
  const [assets, setAssets] = useState<Video[]>([]);
  const [kind, setKind] = useState<CreationKind>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = () => Promise.all([getCatalogTitles(), getVideos(1, 100)]).then(([catalog, library]) => {
    setTitles(catalog);
    setAssets(library.videos.filter((video) => video.status === 'ready'));
  });
  useEffect(() => { void refresh().catch((err) => setError(err.message)); }, []);

  async function create(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!kind) return;
    const form = new FormData(event.currentTarget);
    setBusy(true); setError(null);
    try {
      const title = await createCatalogTitle({
        type: kind,
        title: String(form.get('title') || ''),
        synopsis: String(form.get('synopsis') || ''),
        genres: String(form.get('genres') || '').split(',').map((genre) => genre.trim()).filter(Boolean),
        releaseDate: String(form.get('releaseDate') || '') || undefined,
        maturityRating: String(form.get('maturityRating') || ''),
        posterUrl: String(form.get('posterUrl') || ''),
        backdropUrl: String(form.get('backdropUrl') || ''),
      });
      if (kind === 'movie') {
        const videoId = String(form.get('videoId') || '');
        if (!videoId) throw new Error('Choose a ready upload for this movie.');
        await createPlayable(title.id, { videoId, type: 'movie', title: title.title, synopsis: title.synopsis || '', artworkUrl: title.posterUrl || '' });
      }
      setKind(null);
      await refresh();
      router.push(`/catalog/${title.id}`);
    } catch (err) { setError(err instanceof Error ? err.message : String(err)); }
    finally { setBusy(false); }
  }

  return <div className="space-y-8">
    <section className="flex flex-col gap-4 border-b border-border pb-6 sm:flex-row sm:items-end sm:justify-between">
      <div><p className="text-xs font-semibold uppercase tracking-[0.18em] text-accent">Creator studio</p><h1 className="mt-2 text-3xl font-bold tracking-tight">Stories, organized for release.</h1><p className="mt-2 max-w-2xl text-sm text-muted-foreground">Turn your ready uploads into standalone films or a season-by-season series. Publishing creates an unlisted, revocable viewing link.</p></div>
      <div className="flex gap-2"><button onClick={() => setKind('movie')} className="inline-flex items-center gap-2 rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-accent-foreground"><Film className="h-4 w-4" /> New movie</button><button onClick={() => setKind('series')} className="inline-flex items-center gap-2 rounded-lg border border-border px-4 py-2 text-sm font-semibold hover:bg-muted"><Tv className="h-4 w-4" /> New series</button></div>
    </section>
    {error && <p className="rounded-lg border border-red-500/40 bg-red-500/10 p-3 text-sm text-red-300">{error}</p>}
    {kind && <form onSubmit={create} className="grid gap-4 rounded-xl border border-border bg-card p-5 md:grid-cols-2">
      <div className="md:col-span-2"><p className="text-sm font-semibold">{kind === 'movie' ? 'New movie' : 'New series'}</p><p className="mt-1 text-xs text-muted-foreground">{kind === 'movie' ? 'A movie is one ready video asset.' : 'Create the series shell first, then add seasons and episodes.'}</p></div>
      <input required name="title" placeholder="Title" className="rounded-md border border-border bg-background px-3 py-2 text-sm" />
      <input name="releaseDate" type="date" className="rounded-md border border-border bg-background px-3 py-2 text-sm" />
      <input name="genres" placeholder="Genres, comma separated" className="rounded-md border border-border bg-background px-3 py-2 text-sm" />
      <input name="maturityRating" placeholder="Maturity rating (for example, 16+)" className="rounded-md border border-border bg-background px-3 py-2 text-sm" />
      <input name="posterUrl" type="url" placeholder="Poster image URL" className="rounded-md border border-border bg-background px-3 py-2 text-sm" />
      <input name="backdropUrl" type="url" placeholder="Backdrop image URL" className="rounded-md border border-border bg-background px-3 py-2 text-sm" />
      <textarea name="synopsis" placeholder="Synopsis" className="min-h-24 rounded-md border border-border bg-background px-3 py-2 text-sm md:col-span-2" />
      {kind === 'movie' && <select required name="videoId" defaultValue="" className="rounded-md border border-border bg-background px-3 py-2 text-sm md:col-span-2"><option value="" disabled>Select a ready upload</option>{assets.map((asset) => <option key={asset.id} value={asset.id}>{asset.title} {asset.duration ? `(${Math.round(asset.duration / 60)} min)` : ''}</option>)}</select>}
      <div className="flex justify-end gap-2 md:col-span-2"><button type="button" onClick={() => setKind(null)} className="rounded-md px-3 py-2 text-sm text-muted-foreground">Cancel</button><button disabled={busy} className="rounded-md bg-accent px-4 py-2 text-sm font-semibold text-accent-foreground disabled:opacity-50">{busy ? 'Creating…' : 'Create draft'}</button></div>
    </form>}
    <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">{titles.map((item) => <Link key={item.id} href={`/catalog/${item.id}`} className="group rounded-xl border border-border bg-card p-5 transition-colors hover:border-accent/60 hover:bg-muted/35"><div className="flex items-start justify-between"><span className="rounded-full bg-muted px-2 py-1 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">{item.type}</span><span className={item.status === 'published' ? 'text-xs text-emerald-400' : 'text-xs text-muted-foreground'}>{item.status}</span></div><h2 className="mt-6 text-lg font-semibold group-hover:text-accent">{item.title}</h2><p className="mt-2 line-clamp-2 text-sm text-muted-foreground">{item.synopsis || 'No synopsis yet.'}</p><div className="mt-5 flex items-center gap-2 text-xs text-muted-foreground"><Clapperboard className="h-4 w-4" /> {item.genres?.join(' · ') || 'Editorial draft'}</div></Link>)}</section>
    {!titles.length && !error && <div className="rounded-xl border border-dashed border-border p-12 text-center text-sm text-muted-foreground"><Plus className="mx-auto mb-3 h-5 w-5" />Create your first movie or series from the ready uploads in your library.</div>}
  </div>;
}
