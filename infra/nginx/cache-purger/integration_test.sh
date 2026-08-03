#!/usr/bin/env sh
set -eu

target_id="4c106c1b-0859-4b16-a053-210f52264ead"
sibling_id="9b79a3d3-e701-4bbc-89f1-859ab13e4806"
corrupt_id="a8925a79-cb8d-4d7c-a790-470485773a3e"
secret="0123456789abcdef0123456789abcdef"
suffix="$$-$(date +%s)"
network="hls-purge-it-$suffix"
volume="hls-purge-it-$suffix"
purger="hls-purger-it-$suffix"
origin="hls-origin-it-$suffix"
edge="hls-edge-it-$suffix"
purger_image="hls-cache-purger-it:$suffix"
origin_image="hls-cache-origin-it:$suffix"
edge_image="hls-nginx-it:$suffix"
slow_output="cache-purge-slow-$suffix.tmp"
slow_pid=""

cleanup() {
    if [ -n "$slow_pid" ]; then
        kill "$slow_pid" >/dev/null 2>&1 || true
    fi
    docker rm -f "$edge" "$origin" "$purger" >/dev/null 2>&1 || true
    docker volume rm "$volume" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
    rm -f "$slow_output"
}
trap cleanup EXIT INT TERM

start_purger() {
    docker run -d \
        --name "$purger" \
        --network "$network" \
        --read-only \
        --tmpfs /tmp:size=16m,mode=1777 \
        --cap-drop ALL \
        --cap-add CHOWN \
        --cap-add DAC_OVERRIDE \
        --cap-add SETGID \
        --cap-add SETUID \
        --security-opt no-new-privileges \
        -e "NGINX_CACHE_PURGE_SECRET=$secret" \
        -e NGINX_CACHE_PURGE_BATCH_FILES=2 \
        -e NGINX_CACHE_PURGE_BATCH_DURATION=20ms \
        -e NGINX_CACHE_PURGE_BATCH_PAUSE=5ms \
        -e NGINX_CACHE_PURGE_SWEEP_INTERVAL=2s \
        -e NGINX_CACHE_PURGE_RETRY_DELAY=100ms \
        -e NGINX_CACHE_PURGE_STALE_AFTER=5s \
        -v "$volume:/var/cache/nginx" \
        "$purger_image" >/dev/null
}

wait_http_200() {
    url="$1"
    attempt=0
    while [ "$attempt" -lt 80 ]; do
        if [ "$(curl -sS -o /dev/null -w '%{http_code}' "$url")" = "200" ]; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 0.25
    done
    return 1
}

target_cache_count() {
    docker exec "$edge" sh -c \
        "find /var/cache/nginx/hls -type f -exec grep -a -E -l 'KEY: [^/]+/hls/$target_id/' {} \\; | wc -l" |
        tr -d '[:space:]'
}

sibling_cache_count() {
    docker exec "$edge" sh -c \
        "find /var/cache/nginx/hls -type f -exec grep -a -E -l 'KEY: [^/]+/hls/$sibling_id/' {} \\; | wc -l" |
        tr -d '[:space:]'
}

purge_status() {
    docker exec "$edge" sh -c \
        "wget -q --header='Authorization: Bearer $secret' -O - http://$purger:8080/v1/purge/$target_id"
}

docker build -q -t "$purger_image" infra/nginx/cache-purger >/dev/null
docker build -q -t "$origin_image" infra/nginx/cache-purger/testorigin >/dev/null
docker build -q -t "$edge_image" infra/nginx >/dev/null
docker network create "$network" >/dev/null
docker volume create "$volume" >/dev/null
start_purger

docker run -d \
    --name "$origin" \
    --network "$network" \
    "$origin_image" >/dev/null

docker run -d \
    --name "$edge" \
    --network "$network" \
    -p 127.0.0.1::80 \
    -e "FRONTEND_HOST=$origin" \
    -e FRONTEND_PORT=8080 \
    -e "GATEWAY_HOST=$origin" \
    -e GATEWAY_PORT=8080 \
    -e "CDN_ORIGIN_HOST=$origin" \
    -e CDN_ORIGIN_PORT=8080 \
    -e "CACHE_PURGER_HOST=$purger" \
    -e CACHE_PURGER_PORT=8080 \
    -v "$volume:/var/cache/nginx" \
    "$edge_image" >/dev/null

binding="$(docker port "$edge" 80/tcp)"
port="${binding##*:}"
base_url="http://127.0.0.1:$port"
if ! wait_http_200 "$base_url/nginx-health"; then
    docker logs "$purger"
    docker logs "$edge"
    echo "isolated nginx did not become healthy" >&2
    exit 1
fi

docker exec "$purger" sh -c \
    'grep -Eq "^Uid:[[:space:]]+101[[:space:]]+101[[:space:]]+101[[:space:]]+101" /proc/1/status'
docker exec "$edge" su nginx -s /bin/sh -c \
    'test -w /var/cache/nginx/hls'

target_url="$base_url/hls/$target_id/segment.m4s?token=old-signed-token"
slow_url="$base_url/hls/$target_id/slow.m4s?token=old-slow-token"
sibling_url="$base_url/hls/$sibling_id/segment.m4s?token=sibling-token"
query_sibling_url="$base_url/hls/$sibling_id/query.m4s?next=/hls/$target_id/asset.m4s"
nested_sibling_url="$base_url/hls/$sibling_id/redirect/hls/$target_id/asset.m4s"

for url in "$target_url" "$sibling_url" "$query_sibling_url" "$nested_sibling_url"; do
    first="$(curl -sS -D - -o /dev/null "$url")"
    second="$(curl -sS -D - -o /dev/null "$url")"
    printf '%s' "$first" | grep -qi '^X-Cache-Status: MISS'
    printf '%s' "$second" | grep -qi '^X-Cache-Status: HIT'
done

# This request passes the guard and then blocks at the origin before response
# headers. It is the cache-fill race that a two-pass request-time purge misses.
curl -sS "$slow_url" -o "$slow_output" &
slow_pid="$!"
attempt=0
while [ "$attempt" -lt 40 ]; do
    if docker exec "$edge" wget -q -O /dev/null "http://$origin:8080/control/started"; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 0.1
done
if [ "$attempt" -ge 40 ]; then
    echo "slow origin request did not start" >&2
    exit 1
fi

purge_result="$(
    docker exec "$edge" sh -c \
        "wget -q --header='Authorization: Bearer $secret' --post-data='' -O - http://$purger:8080/v1/purge/$target_id"
)"
printf '%s' "$purge_result" | grep -q "\"video_id\":\"$target_id\""
printf '%s' "$purge_result" | grep -q '"revoked":true'
printf '%s' "$purge_result" | grep -Eq '"required_generation":[1-9][0-9]*'

# Revocation is immediate even while physical convergence is asynchronous.
[ "$(curl -sS -o /dev/null -w '%{http_code}' "$target_url")" = "403" ]
attempt=0
while [ "$attempt" -lt 50 ]; do
    status="$(purge_status)"
    if printf '%s' "$status" | grep -q '"state":"converged"'; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 0.1
done
if [ "$attempt" -ge 50 ]; then
    echo "initial cache inventory did not converge" >&2
    exit 1
fi
[ "$(target_cache_count)" = "0" ]

# Release the old in-flight response only after the first full inventory pass.
# It creates a late cache file, which the durable periodic reconciler must
# discover and remove without ever reopening playback.
docker exec "$edge" sh -c \
    "wget -q --post-data='' -O /dev/null http://$origin:8080/control/release"
wait "$slow_pid"
slow_pid=""
[ "$(cat "$slow_output")" = "media-bytes!" ]

late_count="$(target_cache_count)"
if [ "$late_count" -lt 1 ]; then
    echo "slow origin did not produce the intended post-convergence cache fill" >&2
    exit 1
fi
attempt=0
while [ "$attempt" -lt 80 ]; do
    if [ "$(target_cache_count)" = "0" ]; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 0.1
done
if [ "$attempt" -ge 80 ]; then
    echo "late cache fill did not converge to zero" >&2
    exit 1
fi
[ "$(curl -sS -o /dev/null -w '%{http_code}' "$slow_url")" = "403" ]

# Query and nested sibling references to the target UUID are not target paths.
for url in "$sibling_url" "$query_sibling_url" "$nested_sibling_url"; do
    headers="$(curl -sS -D - -o /dev/null "$url")"
    printf '%s' "$headers" | grep -qi '^X-Cache-Status: HIT'
done
[ "$(sibling_cache_count)" = "3" ]

# Recreate the purger against the same durable volume. The new process loads
# the tombstone before becoming healthy and keeps the target denied.
docker rm -f "$purger" >/dev/null
start_purger
attempt=0
while [ "$attempt" -lt 80 ]; do
    code="$(curl -sS -o /dev/null -w '%{http_code}' "$base_url/nginx-health")"
    if [ "$code" = "200" ]; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 0.25
done
if [ "$attempt" -ge 80 ]; then
    echo "recreated cache purger did not become healthy" >&2
    exit 1
fi
[ "$(curl -sS -o /dev/null -w '%{http_code}' "$target_url")" = "403" ]
headers="$(curl -sS -D - -o /dev/null "$sibling_url")"
printf '%s' "$headers" | grep -qi '^X-Cache-Status: HIT'
[ "$(target_cache_count)" = "0" ]

# Corrupt or unavailable revocation storage is uncertainty, not evidence that
# a video is safe. Both cases must fail closed even for a would-be cache HIT.
docker exec "$purger" mkdir "/var/cache/nginx/tombstones/$corrupt_id"
corrupt_code="$(
    curl -sS -o /dev/null -w '%{http_code}' \
        "$base_url/hls/$corrupt_id/segment.m4s?token=old"
)"
[ "$corrupt_code" != "200" ]
docker exec "$purger" rmdir "/var/cache/nginx/tombstones/$corrupt_id"

docker exec "$purger" mv \
    /var/cache/nginx/tombstones \
    /var/cache/nginx/tombstones-unavailable
[ "$(curl -sS -o /dev/null -w '%{http_code}' "$sibling_url")" != "200" ]
docker exec "$purger" mv \
    /var/cache/nginx/tombstones-unavailable \
    /var/cache/nginx/tombstones
if ! wait_http_200 "$base_url/nginx-health"; then
    echo "cache purger did not recover after tombstone storage was restored" >&2
    exit 1
fi
headers="$(curl -sS -D - -o /dev/null "$sibling_url")"
printf '%s' "$headers" | grep -qi '^X-Cache-Status: HIT'

# Guard availability remains part of edge health, and cache HITs fail closed if
# the guard disappears.
docker stop "$purger" >/dev/null
[ "$(curl -sS -o /dev/null -w '%{http_code}' "$base_url/nginx-health")" != "200" ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' "$sibling_url")" != "200" ]

echo "cache purge slow-fill integration passed"
