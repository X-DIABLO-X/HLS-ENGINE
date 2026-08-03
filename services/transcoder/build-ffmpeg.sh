#!/bin/sh
set -eu

: "${FFMPEG_VERSION:?FFMPEG_VERSION is required}"
: "${FFMPEG_SHA256:?FFMPEG_SHA256 is required}"
: "${FFMPEG_BUILD_JOBS:=4}"
: "${SOURCE_DATE_EPOCH:=0}"

build_root="$(mktemp -d)"
trap 'rm -rf "$build_root"' EXIT HUP INT TERM

archive="$build_root/ffmpeg.tar.xz"
source_dir="$build_root/source"

curl \
  --connect-timeout 30 \
  --fail \
  --location \
  --proto '=https' \
  --retry 5 \
  --retry-all-errors \
  --retry-delay 2 \
  --show-error \
  --silent \
  "https://ffmpeg.org/releases/ffmpeg-${FFMPEG_VERSION}.tar.xz" \
  --output "$archive"

printf '%s  %s\n' "$FFMPEG_SHA256" "$archive" | sha256sum --check --strict

mkdir -p "$source_dir"
tar --extract --xz --file "$archive" --directory "$source_dir" --strip-components=1
cd "$source_dir"

export SOURCE_DATE_EPOCH

# Debian 13's libplacebo helper headers target FFmpeg 7's AVStream side-data
# API and do not compile against FFmpeg 8.1. The worker does not use that
# optional filter, so keep the runtime package installed but disable the
# integration until Debian ships matching headers.
./configure \
  --prefix=/opt/ffmpeg \
  --disable-debug \
  --disable-doc \
  --disable-ffplay \
  --enable-gpl \
  --enable-gnutls \
  --enable-cuda-llvm \
  --enable-ffnvcodec \
  --enable-cuvid \
  --enable-nvenc \
  --enable-libaom \
  --enable-libass \
  --enable-libbs2b \
  --enable-libcdio \
  --enable-libcodec2 \
  --enable-libdav1d \
  --enable-libflite \
  --enable-libfontconfig \
  --enable-libfreetype \
  --enable-libfribidi \
  --enable-libglslang \
  --enable-libgme \
  --enable-libgsm \
  --enable-libharfbuzz \
  --enable-libjxl \
  --enable-libmp3lame \
  --enable-libmysofa \
  --enable-libopenjpeg \
  --enable-libopenmpt \
  --enable-libopus \
  --disable-libplacebo \
  --enable-librav1e \
  --enable-librsvg \
  --enable-librubberband \
  --enable-libshine \
  --enable-libsnappy \
  --enable-libsoxr \
  --enable-libspeex \
  --enable-libsvtav1 \
  --enable-libtheora \
  --enable-libtwolame \
  --enable-libvidstab \
  --enable-libvorbis \
  --enable-libvpl \
  --enable-libvpx \
  --enable-libwebp \
  --enable-libx264 \
  --enable-libx265 \
  --enable-libxml2 \
  --enable-libxvid \
  --enable-libzimg \
  --enable-libzmq \
  --enable-openal \
  --enable-opencl \
  --enable-opengl \
  --disable-sndio \
  --enable-libdc1394 \
  --enable-libdrm \
  --enable-libiec61883 \
  --enable-chromaprint \
  --enable-frei0r \
  --enable-ladspa \
  --enable-libbluray \
  --enable-libcaca \
  --enable-libdvdnav \
  --enable-libdvdread \
  --enable-libjack \
  --enable-libpulse \
  --enable-librabbitmq \
  --enable-librist \
  --enable-libsrt \
  --enable-libssh \
  --enable-libzvbi \
  --enable-lv2 \
  --enable-pocketsphinx \
  --enable-sdl2

make -j"$FFMPEG_BUILD_JOBS" ffmpeg ffprobe

install -d /opt/ffmpeg/bin
install -m 0755 ffmpeg ffprobe /opt/ffmpeg/bin/
strip --strip-unneeded /opt/ffmpeg/bin/ffmpeg /opt/ffmpeg/bin/ffprobe

/opt/ffmpeg/bin/ffmpeg -hide_banner -version
/opt/ffmpeg/bin/ffmpeg -hide_banner -encoders 2>/dev/null | grep -q 'h264_nvenc'
/opt/ffmpeg/bin/ffmpeg -hide_banner -filters 2>/dev/null | grep -q 'scale_cuda'
