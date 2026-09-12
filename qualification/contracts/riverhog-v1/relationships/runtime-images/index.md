# Runtime-image composition

[Atlas](../../index.md) · [Relationships](../index.md)

Image purpose and composition are the exact release metadata; reference images remain optional and nonnormative.

Runtime images: **13**

| Image | Role | Purpose | Description source | Packaged components |
|---|---|---|---|---|
| <a id="node-image-runtime-mango-fish"></a>`mango-fish` | `reference` | Optional nonnormative CloudEvents reference application for Riverhog. | `release.toml#/images/runtime/mango-fish/description` | [mango-fish](../components/mango-fish.md) |
| <a id="node-image-runtime-riverhog"></a>`riverhog` | `product` | Riverhog archive service. | `release.toml#/images/runtime/riverhog/description` | [riverhog-server](../components/riverhog-server.md) |
| <a id="node-image-runtime-riverhog-ftp-adapter"></a>`riverhog-ftp-adapter` | `reference` | Optional nonnormative Riverhog FTP ingress reference. | `release.toml#/images/runtime/riverhog-ftp-adapter/description` | [riverhog-ftp-adapter](../components/riverhog-ftp-adapter.md), [riverhog-provenance-linux-observer](../components/riverhog-provenance-linux-observer.md) |
| <a id="node-image-runtime-riverhog-storage-adapter-aws"></a>`riverhog-storage-adapter-aws` | `reference` | Optional nonnormative AWS storage reference for Riverhog. | `release.toml#/images/runtime/riverhog-storage-adapter-aws/description` | [riverhog-storage-adapter-aws](../components/riverhog-storage-adapter-aws.md) |
| <a id="node-image-runtime-riverhog-storage-adapter-backblaze"></a>`riverhog-storage-adapter-backblaze` | `reference` | Optional nonnormative Backblaze B2 storage reference for Riverhog. | `release.toml#/images/runtime/riverhog-storage-adapter-backblaze/description` | [riverhog-storage-adapter-backblaze](../components/riverhog-storage-adapter-backblaze.md) |
| <a id="node-image-runtime-riverhog-storage-adapter-filesystem"></a>`riverhog-storage-adapter-filesystem` | `reference` | Optional nonnormative Linux filesystem storage reference for Riverhog. | `release.toml#/images/runtime/riverhog-storage-adapter-filesystem/description` | [riverhog-storage-adapter-filesystem](../components/riverhog-storage-adapter-filesystem.md) |
| <a id="node-image-runtime-stove0"></a>`stove0` | `reference` | Optional nonnormative transformation reference application for Riverhog. | `release.toml#/images/runtime/stove0/description` | [stove0-server](../components/stove0-server.md) |
| <a id="node-image-runtime-stove0-exiftool-observer"></a>`stove0-exiftool-observer` | `reference` | Optional nonnormative ExifTool observer reference for Stove0. | `release.toml#/images/runtime/stove0-exiftool-observer/description` | [stove0-exiftool-observer](../components/stove0-exiftool-observer.md) |
| <a id="node-image-runtime-stove0-ffprobe-sampling-observer"></a>`stove0-ffprobe-sampling-observer` | `reference` | Optional nonnormative FFprobe sampling-observer reference for Stove0. | `release.toml#/images/runtime/stove0-ffprobe-sampling-observer/description` | [stove0-ffprobe-sampling-observer](../components/stove0-ffprobe-sampling-observer.md) |
| <a id="node-image-runtime-stove0-nvenc-av1-opus-target"></a>`stove0-nvenc-av1-opus-target` | `reference` | Optional nonnormative NVENC AV1 and Opus target reference for Stove0. | `release.toml#/images/runtime/stove0-nvenc-av1-opus-target/description` | [stove0-nvenc-av1-opus-review-sampler](../components/stove0-nvenc-av1-opus-review-sampler.md), [stove0-nvenc-av1-opus-target](../components/stove0-nvenc-av1-opus-target.md) |
| <a id="node-image-runtime-stove0-opus-target"></a>`stove0-opus-target` | `reference` | Optional nonnormative Opus target reference for Stove0. | `release.toml#/images/runtime/stove0-opus-target/description` | [stove0-opus-review-sampler](../components/stove0-opus-review-sampler.md), [stove0-opus-target](../components/stove0-opus-target.md) |
| <a id="node-image-runtime-stove0-review-materialize-target"></a>`stove0-review-materialize-target` | `reference` | Optional nonnormative review materialization target reference for Stove0. | `release.toml#/images/runtime/stove0-review-materialize-target/description` | [stove0-review-materialize-target](../components/stove0-review-materialize-target.md) |
| <a id="node-image-runtime-stove0-review-rclone-effect-target"></a>`stove0-review-rclone-effect-target` | `reference` | Optional nonnormative rclone review-effect target reference for Stove0. | `release.toml#/images/runtime/stove0-review-rclone-effect-target/description` | [stove0-review-rclone-effect-target](../components/stove0-review-rclone-effect-target.md) |
