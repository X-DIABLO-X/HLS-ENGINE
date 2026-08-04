import assert from 'node:assert/strict';
import test from 'node:test';

import { resolveManifestUrl } from '../lib/manifest-url.mjs';

const signed = '/hls/video-1/master.m3u8?token=expiry%3Asignature';

test('relative signed manifests remain on the browser origin by default', () => {
  assert.equal(resolveManifestUrl(signed, undefined), signed);
  assert.equal(resolveManifestUrl(signed, ''), signed);
});

test('an explicit CDN origin preserves the signed path and query', () => {
  assert.equal(
    resolveManifestUrl(signed, 'https://media.example.com/hls'),
    'https://media.example.com/hls/video-1/master.m3u8'
      + '?token=expiry%3Asignature'
  );
});

test('invalid or non-HTTP public origins fail back to same-origin playback', () => {
  assert.equal(resolveManifestUrl(signed, 'not a URL'), signed);
  assert.equal(resolveManifestUrl(signed, 'javascript:alert(1)'), signed);
  assert.equal(resolveManifestUrl(signed, 'https://user:pass@example.com'), signed);
});
