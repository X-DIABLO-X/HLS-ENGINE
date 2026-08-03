# Nginx cache purger

This internal sidecar closes the gap between durable video deletion and
Nginx's disk cache.

The metadata service first commits its database `deleting` tombstone, then
sends an authenticated `POST /v1/purge/{videoID}` request. The sidecar:

1. validates and canonicalizes the UUID;
2. atomically writes and syncs a permanent per-video revocation tombstone;
3. blocks new HLS requests for that UUID through Nginx's `auth_request` guard;
4. returns `202 Accepted` only after that durable fail-closed boundary exists;
5. enqueues the UUID for a coalesced background inventory of the configured
   cache root.

It never accepts a filesystem path from the caller, never follows symlinks,
and runs as the unprivileged Nginx worker uid after its two fixed directories
are initialized. The service has no published host port.

The reconciler parses the first request-path video UUID from each Nginx `KEY:`
header; target-looking text later in a sibling path or query is not a match.
One inventory pass coalesces all tombstoned UUIDs. Work is yielded after a
configurable file-count or time batch, but the in-memory iterator resumes until
the complete inventory succeeds. There is no hard total-file ceiling.

Successful convergence is observable at authenticated
`GET /v1/purge/{videoID}`. Failed inventories report `retrying`, degrade
`/health`, and retry without a caller context. Periodic complete inventories
also reclaim a cache file written late by an origin request that passed the
guard before revocation. The durable guard tombstone never gets removed, so
these retries cannot reopen playback and survive process restarts.

If durable revocation fails, metadata leaves the video as `deleting` and
returns a retryable response. Origin objects are not removed until revocation
succeeds. Physical disk convergence is asynchronous and remains observable
after the metadata row is finalized.

Run unit tests with:

```sh
go test -race ./...
go vet ./...
```

Run the isolated live Nginx integration on a Linux Docker host with:

```sh
sh integration_test.sh
```

The integration holds an origin response across revocation, proves the first
inventory completes before that response creates a late cache file, and then
waits for periodic convergence to remove it. It also preserves sibling entries
whose path or query mentions the target, recreates the purger from the same
volume, and confirms the edge fails closed if the guard is unavailable.
