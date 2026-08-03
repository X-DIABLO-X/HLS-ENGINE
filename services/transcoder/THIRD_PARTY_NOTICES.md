# Transcoder image licensing and source

The HLS-ENGINE application source is licensed under the repository's MIT
License. The transcoder containers are combined distributions that also
contains independently licensed software. The MIT License does not replace
those licenses.

## FFmpeg

The GPU image builds FFmpeg 8.1.2 from the official release archive:

- Source: <https://ffmpeg.org/releases/ffmpeg-8.1.2.tar.xz>
- SHA-256:
  `464beb5e7bf0c311e68b45ae2f04e9cc2af88851abb4082231742a74d97b524c`
- Exact configure and build procedure:
  [`build-ffmpeg.sh`](build-ffmpeg.sh)

The build enables GPL components, including `libx264` and `libx265`.
Consequently, the resulting FFmpeg executables are distributed under the GNU
General Public License, version 2 or later; they are not covered by the
HLS-ENGINE MIT License. FFmpeg's licensing guidance is available at
<https://ffmpeg.org/legal.html>.

Anyone publishing a prebuilt image must satisfy the applicable source-code,
copyright-notice, and license-delivery requirements. At minimum, publish the
exact FFmpeg archive above, this build script, and the corresponding source for
the exact Debian libraries present in that image alongside the binary image.
Do not describe the complete container image as MIT-only.

## Debian packages and codec libraries

Both transcoder images use Debian 13 packages, including dynamically linked
codec libraries. The CPU image runs Debian's packaged FFmpeg; the GPU image
uses those runtime libraries with the source-built FFmpeg described above.
Package versions can change when an image is rebuilt because the Debian
repositories are not snapshot-pinned. Record the exact binary package
inventory for every published image:

```sh
dpkg-query -W -f='${Package}=${Version}\n' | sort
```

The corresponding Debian source packages and their license files must be
retained for the published build. Debian source-package instructions are at
<https://www.debian.org/doc/manuals/maint-guide/build.en.html>, and historical
versions are available from <https://snapshot.debian.org/>.

Package-specific copyright and license texts remain in
`/usr/share/doc/<package>/copyright` inside the image.

## NVIDIA interfaces

NVENC, NVDEC, CUDA interfaces, and NVIDIA codec headers remain subject to
NVIDIA's licenses. HLS-ENGINE does not bundle an NVIDIA display driver; the
NVIDIA Container Toolkit injects compatible host driver libraries at runtime.

This notice is operational guidance, not legal advice. A distributor remains
responsible for reviewing the exact image contents and satisfying every
applicable license.
