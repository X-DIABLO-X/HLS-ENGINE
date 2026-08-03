/**
 * Resolve a signed manifest against an optional public HLS/CDN origin.
 *
 * With no explicit origin, keep the relative signed URL so it follows the
 * browser's actual host and port. When an origin is configured, preserve only
 * the signed path/query/fragment and place them on that trusted HTTP(S)
 * origin; a malformed setting safely falls back to same-origin playback.
 *
 * @param {string} signedUrl
 * @param {string | undefined} configuredBase
 * @returns {string}
 */
export function resolveManifestUrl(
  signedUrl,
  configuredBase = process.env.NEXT_PUBLIC_HLS_BASE_URL
) {
  const base = configuredBase?.trim();
  if (!base) return signedUrl;

  try {
    const publicOrigin = new URL(base);
    if (
      !['http:', 'https:'].includes(publicOrigin.protocol) ||
      publicOrigin.username ||
      publicOrigin.password
    ) {
      return signedUrl;
    }

    const signed = new URL(signedUrl, 'https://hls-engine.invalid');
    publicOrigin.pathname = signed.pathname;
    publicOrigin.search = signed.search;
    publicOrigin.hash = signed.hash;
    return publicOrigin.href;
  } catch {
    return signedUrl;
  }
}
