"""MinIO upload/download helpers and atomic publish."""

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import os
import posixpath
import re
import shutil
import time
from typing import List, Optional, Tuple
from urllib.parse import unquote, urlsplit

import httpx
from minio import Minio
from minio.error import S3Error

from app.config import get_settings

logger = logging.getLogger(__name__)

_MASTER_PLAYLIST = "master.m3u8"
_DEFAULT_PUBLISH_WORKERS = 8
_MAX_PUBLISH_WORKERS = 32
_URI_ATTRIBUTE_RE = re.compile(
    r'(?:^|,)\s*URI\s*=\s*(?:"([^"]*)"|([^,\s]*))',
    re.IGNORECASE,
)


def get_client() -> Minio:
    settings = get_settings()
    return Minio(
        settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=settings.MINIO_SECURE,
        region=settings.MINIO_REGION or None,
    )


def ensure_bucket() -> None:
    settings = get_settings()
    client = get_client()
    if not client.bucket_exists(settings.MINIO_BUCKET):
        client.make_bucket(settings.MINIO_BUCKET)


def parse_minio_url(url: str) -> Optional[Tuple[str, str]]:
    if url.startswith("minio://"):
        rest = url[len("minio://") :]
        if "/" in rest:
            bucket, key = rest.split("/", 1)
            return bucket, key
    return None


def download_source(url: str, dest_path: str) -> None:
    """Download a source URL to a local path.

    Supports:
      - minio://bucket/key
      - http(s)://...
      - local filesystem path (copy)
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    parsed = parse_minio_url(url)
    if parsed:
        bucket, key = parsed
        get_client().fget_object(bucket, key, dest_path)
        return

    if url.startswith(("http://", "https://")):
        with httpx.Client(follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()
            with open(dest_path, "wb") as f:
                f.write(response.content)
        return

    if os.path.exists(url):
        shutil.copy(url, dest_path)
        return

    raise FileNotFoundError(f"Cannot resolve source URL: {url}")


def upload_directory(local_dir: str, prefix: str) -> List[str]:
    """Recursively upload a local directory to a MinIO prefix."""
    client = get_client()
    bucket = get_settings().MINIO_BUCKET
    uploaded: List[str] = []
    for root, _, files in os.walk(local_dir):
        for name in files:
            full_path = os.path.join(root, name)
            rel = os.path.relpath(full_path, local_dir).replace("\\", "/")
            object_name = f"{prefix.rstrip('/')}/{rel}"
            client.fput_object(bucket, object_name, full_path)
            uploaded.append(object_name)
    return uploaded


def delete_prefix(prefix: str) -> None:
    client = get_client()
    bucket = get_settings().MINIO_BUCKET
    objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    for obj in objects:
        client.remove_object(bucket, obj.object_name)


def published_prefix_exists(prefix: str) -> bool:
    """Return whether a published prefix has a non-empty commit marker.

    ``master.m3u8`` is the publication commit marker.  A zero-byte object is
    not a valid playlist and must never make an interrupted publish appear
    complete.
    """
    object_prefix = _normalized_object_prefix(prefix)
    object_name = f"{object_prefix}/{_MASTER_PLAYLIST}"
    client = get_client()
    try:
        stat = client.stat_object(get_settings().MINIO_BUCKET, object_name)
        return int(getattr(stat, "size", 0) or 0) > 0
    except S3Error as exc:
        if exc.code in {
            "NoSuchKey",
            "NoSuchObject",
            "NoSuchBucket",
            "NotFound",
        }:
            return False
        raise


def withhold_published_prefix(prefix: str) -> None:
    """Remove a presentation's public commit marker idempotently."""
    object_prefix = _normalized_object_prefix(prefix)
    _withhold_master(
        get_client(),
        get_settings().MINIO_BUCKET,
        f"{object_prefix}/{_MASTER_PLAYLIST}",
    )


def _publish_worker_count() -> int:
    raw_value = os.getenv(
        "HLS_PUBLISH_MAX_WORKERS",
        str(_DEFAULT_PUBLISH_WORKERS),
    )
    try:
        requested = int(raw_value)
    except (TypeError, ValueError):
        logger.warning(
            "Invalid HLS_PUBLISH_MAX_WORKERS=%r; using %d",
            raw_value,
            _DEFAULT_PUBLISH_WORKERS,
        )
        return _DEFAULT_PUBLISH_WORKERS
    return max(1, min(requested, _MAX_PUBLISH_WORKERS))


def _normalized_object_prefix(prefix: str) -> str:
    normalized = prefix.replace("\\", "/").strip("/")
    if not normalized:
        raise ValueError("final_prefix must not be empty")
    if any(part in {"", ".", ".."} for part in normalized.split("/")):
        raise ValueError(f"final_prefix contains an invalid path segment: {prefix!r}")
    return normalized


def _collect_publish_files(local_dir: str) -> List[Tuple[str, str]]:
    """Return validated ``(local_path, object_suffix)`` pairs.

    Generated HLS output must be self-contained. Refusing links prevents a
    malformed work directory from publishing a file outside ``local_dir``.
    """
    if os.path.islink(local_dir):
        raise ValueError("local_dir must not be a symbolic link")

    root_dir = os.path.abspath(local_dir)
    if not os.path.isdir(root_dir):
        raise FileNotFoundError(f"HLS publish directory does not exist: {local_dir}")

    publish_files: List[Tuple[str, str]] = []
    for root, dirs, files in os.walk(root_dir, followlinks=False):
        dirs.sort()
        files.sort()
        for dirname in dirs:
            if os.path.islink(os.path.join(root, dirname)):
                raise ValueError("HLS publish directory must not contain links")
        for name in files:
            full_path = os.path.join(root, name)
            if os.path.islink(full_path):
                raise ValueError("HLS publish directory must not contain links")
            if not os.path.isfile(full_path):
                raise ValueError(f"HLS publish entry is not a file: {full_path}")

            relative_path = os.path.relpath(full_path, root_dir).replace("\\", "/")
            if relative_path == ".." or relative_path.startswith("../"):
                raise ValueError(f"HLS publish path escapes local_dir: {full_path}")
            publish_files.append((full_path, relative_path))

    return publish_files


def _read_playlist(path: str, relative_path: str) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8-sig") as playlist:
            lines = [line.strip() for line in playlist if line.strip()]
    except (OSError, UnicodeError) as exc:
        raise ValueError(
            f"HLS playlist is missing or unreadable: {relative_path}"
        ) from exc

    if not lines or lines[0] != "#EXTM3U":
        raise ValueError(f"invalid HLS playlist header: {relative_path}")
    return lines


def _attribute_uris(line: str) -> List[str]:
    """Return URI attributes from one HLS tag."""
    if ":" not in line:
        return []
    payload = line.split(":", 1)[1]
    return [
        quoted if quoted != "" else bare
        for quoted, bare in _URI_ATTRIBUTE_RE.findall(payload)
    ]


def _resolve_playlist_reference(
    referring_playlist: str,
    uri: str,
    files_by_name: dict,
) -> str:
    """Resolve one self-contained HLS URI to a collected object suffix."""
    if not uri or any(char in uri for char in ("\x00", "\r", "\n", "\\")):
        raise ValueError(
            f"invalid HLS URI in {referring_playlist}: {uri!r}"
        )

    parsed = urlsplit(uri)
    if (
        parsed.scheme
        or parsed.netloc
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            f"HLS URI must be a local object in {referring_playlist}: {uri}"
        )

    decoded = unquote(parsed.path)
    if not decoded or decoded.startswith("/"):
        raise ValueError(
            f"invalid HLS URI in {referring_playlist}: {uri!r}"
        )

    resolved = posixpath.normpath(
        posixpath.join(posixpath.dirname(referring_playlist), decoded)
    )
    if (
        resolved in {"", ".", ".."}
        or resolved.startswith("../")
        or resolved.startswith("/")
    ):
        raise ValueError(
            f"HLS URI escapes the publish directory in "
            f"{referring_playlist}: {uri}"
        )
    if resolved not in files_by_name:
        raise ValueError(
            f"HLS reference is missing from the publish directory: "
            f"{referring_playlist} -> {uri}"
        )
    return resolved


def _validate_media_playlist(
    playlist_name: str,
    files_by_name: dict,
) -> None:
    playlist_path = files_by_name[playlist_name]
    lines = _read_playlist(playlist_path, playlist_name)
    if any(line.startswith("#EXT-X-STREAM-INF:") for line in lines):
        raise ValueError(
            f"master playlist cannot be used as a media rendition: "
            f"{playlist_name}"
        )
    if "#EXT-X-ENDLIST" not in lines:
        raise ValueError(
            f"incomplete HLS media playlist (ENDLIST missing): "
            f"{playlist_name}"
        )

    media_uris = [line for line in lines if not line.startswith("#")]
    if not media_uris:
        raise ValueError(
            f"HLS media playlist has no media URI: {playlist_name}"
        )

    extinf_count = sum(
        1 for line in lines if line.startswith("#EXTINF:")
    )
    if extinf_count != len(media_uris):
        raise ValueError(
            f"HLS media playlist has an incomplete segment list: "
            f"{playlist_name}"
        )

    referenced_assets = list(media_uris)
    for line in lines:
        if line.startswith("#"):
            referenced_assets.extend(_attribute_uris(line))

    for uri in referenced_assets:
        asset_name = _resolve_playlist_reference(
            playlist_name, uri, files_by_name
        )
        if asset_name.lower().endswith(".m3u8"):
            raise ValueError(
                f"HLS media playlist references another playlist: "
                f"{playlist_name} -> {uri}"
            )
        try:
            asset_size = os.path.getsize(files_by_name[asset_name])
        except OSError as exc:
            raise ValueError(
                f"HLS media asset is unreadable: {asset_name}"
            ) from exc
        if asset_size <= 0:
            raise ValueError(f"HLS media asset is empty: {asset_name}")


def _validate_hls_presentation(
    files: List[Tuple[str, str]],
) -> None:
    """Validate the complete root master graph before uploading any object."""
    files_by_name = {relative: local for local, relative in files}
    master_path = files_by_name[_MASTER_PLAYLIST]
    lines = _read_playlist(master_path, _MASTER_PLAYLIST)

    rendition_uris: List[str] = []
    auxiliary_uris: List[str] = []
    for index, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF:"):
            if index + 1 >= len(lines) or lines[index + 1].startswith("#"):
                raise ValueError(
                    "master playlist has EXT-X-STREAM-INF without a "
                    "rendition URI"
                )
            rendition_uris.append(lines[index + 1])
        elif line.startswith("#EXT-X-MEDIA:"):
            tag_uris = _attribute_uris(line)
            # AUDIO and SUBTITLES entries emitted by this engine are external
            # renditions and therefore require exactly one playlist URI.
            upper_line = line.upper()
            if (
                ("TYPE=AUDIO" in upper_line or "TYPE=SUBTITLES" in upper_line)
                and len(tag_uris) != 1
            ):
                raise ValueError(
                    "master AUDIO/SUBTITLES entry must reference exactly "
                    "one playlist"
                )
            auxiliary_uris.extend(tag_uris)

    if not rendition_uris:
        raise ValueError(
            "HLS master playlist must reference at least one video rendition"
        )

    master_media_lines = [
        line for line in lines if not line.startswith("#")
    ]
    if master_media_lines != rendition_uris:
        raise ValueError(
            "master playlist contains an unpaired or misplaced rendition URI"
        )

    referenced_playlists = set()
    for uri in rendition_uris + auxiliary_uris:
        playlist_name = _resolve_playlist_reference(
            _MASTER_PLAYLIST, uri, files_by_name
        )
        if not playlist_name.lower().endswith(".m3u8"):
            raise ValueError(
                f"master playlist reference is not a playlist: {uri}"
            )
        referenced_playlists.add(playlist_name)

    for playlist_name in sorted(referenced_playlists):
        _validate_media_playlist(playlist_name, files_by_name)

    collected_playlists = {
        relative
        for _, relative in files
        if relative != _MASTER_PLAYLIST
        and relative.lower().endswith(".m3u8")
    }
    unreferenced = collected_playlists - referenced_playlists
    if unreferenced:
        raise ValueError(
            "publish directory contains unreferenced HLS playlists: "
            + ", ".join(sorted(unreferenced))
        )


def _upload_stage(
    client: Minio,
    bucket: str,
    object_prefix: str,
    files: List[Tuple[str, str]],
    workers: int,
) -> None:
    """Upload one dependency stage and return only after all files are durable."""
    if not files:
        return

    def upload_one(item: Tuple[str, str]) -> None:
        local_path, relative_path = item
        object_name = f"{object_prefix}/{relative_path}"
        client.fput_object(bucket, object_name, local_path)

    # The MinIO Python client is documented as thread-safe for Python threads
    # (but not for multiprocessing), so one client can safely share its HTTP
    # pool across this bounded, in-process executor.
    futures = []
    with ThreadPoolExecutor(
        max_workers=min(workers, len(files)),
        thread_name_prefix="hls-publish",
    ) as executor:
        futures = [executor.submit(upload_one, item) for item in files]
        try:
            for future in as_completed(futures):
                future.result()
        except BaseException:
            for future in futures:
                future.cancel()
            raise


def _withhold_master(client: Minio, bucket: str, master_object: str) -> None:
    """Idempotently remove the public commit marker."""
    client.remove_object(bucket, master_object)


def _remote_inventory(
    client: Minio,
    bucket: str,
    object_prefix: str,
) -> set:
    """Return only objects inside the exact normalized prefix boundary."""
    boundary = f"{object_prefix}/"
    return {
        item.object_name
        for item in client.list_objects(
            bucket,
            prefix=boundary,
            recursive=True,
        )
        if str(item.object_name).startswith(boundary)
    }


def _reconcile_remote_inventory(
    client: Minio,
    bucket: str,
    object_prefix: str,
    expected_objects: set,
) -> None:
    """Remove stale keys and prove the withheld presentation is exact."""
    existing = _remote_inventory(client, bucket, object_prefix)
    for object_name in sorted(existing - expected_objects):
        client.remove_object(bucket, object_name)

    final_inventory = _remote_inventory(client, bucket, object_prefix)
    if final_inventory != expected_objects:
        missing = sorted(expected_objects - final_inventory)
        unexpected = sorted(final_inventory - expected_objects)
        raise RuntimeError(
            "remote HLS inventory does not match the validated local graph "
            f"(missing={missing[:5]}, unexpected={unexpected[:5]})"
        )


def atomic_publish(local_dir: str, final_prefix: str) -> str:
    """Publish HLS with ``master.m3u8`` as a fail-closed commit marker.

    Assets are uploaded first, media playlists only after every asset succeeds,
    and the master playlist strictly last. A retry overwrites the same object
    keys, while any failed attempt removes the master playlist so clients can
    never discover a partially published presentation.
    """
    client = get_client()
    bucket = get_settings().MINIO_BUCKET
    object_prefix = _normalized_object_prefix(final_prefix)
    master_object = f"{object_prefix}/{_MASTER_PLAYLIST}"
    workers = _publish_worker_count()
    started_at = time.monotonic()

    try:
        # Withhold first, before even scanning local content. Once a publish
        # attempt starts, every validation or I/O error must leave it closed.
        # MinIO is strongly consistent, so a successful remove makes the
        # presentation undiscoverable until the final fput below commits it.
        _withhold_master(client, bucket, master_object)

        files = _collect_publish_files(local_dir)
        master_files = [
            item for item in files if item[1] == _MASTER_PLAYLIST
        ]
        if len(master_files) != 1:
            raise ValueError(
                f"HLS publish requires exactly one root {_MASTER_PLAYLIST}; "
                f"found {len(master_files)}"
            )
        _validate_hls_presentation(files)

        media_playlists = [
            item
            for item in files
            if item[1] != _MASTER_PLAYLIST
            and item[1].lower().endswith(".m3u8")
        ]
        assets = [
            item
            for item in files
            if item[1] != _MASTER_PLAYLIST
            and not item[1].lower().endswith(".m3u8")
        ]
        logger.info(
            "Publishing HLS directly to %s: %d assets, %d media playlists, "
            "bounded concurrency=%d",
            final_prefix,
            len(assets),
            len(media_playlists),
            workers,
        )

        _upload_stage(client, bucket, object_prefix, assets, workers)
        _upload_stage(client, bucket, object_prefix, media_playlists, workers)

        # A prior failed/retried generation can contain segment names that the
        # new playlists no longer reference. Reconcile while master is absent,
        # then verify the exact graph before making it discoverable.
        expected_without_master = {
            f"{object_prefix}/{relative_path}"
            for _, relative_path in assets + media_playlists
        }
        _reconcile_remote_inventory(
            client,
            bucket,
            object_prefix,
            expected_without_master,
        )

        master_path, _ = master_files[0]
        client.fput_object(bucket, master_object, master_path)
    except BaseException:
        # A master PUT can succeed server-side even if its response is lost.
        # Removing it again makes ambiguous failures fail closed as well.
        try:
            _withhold_master(client, bucket, master_object)
        except Exception:
            logger.critical(
                "Publication failed and master commit marker could not be "
                "removed: minio://%s/%s",
                bucket,
                master_object,
                exc_info=True,
            )
            raise RuntimeError(
                "HLS publication failed and its master playlist could not "
                "be withheld"
            )
        raise

    logger.info(
        "Published HLS commit marker minio://%s/%s in %.2fs",
        bucket,
        master_object,
        time.monotonic() - started_at,
    )
    return final_prefix
