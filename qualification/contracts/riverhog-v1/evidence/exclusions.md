# Explicitly excluded candidates

[Atlas](../index.md) · [Freeze evidence](index.md) · [Policies](../policies/index.md)

This page supports the ‘no more’ side of the audit by naming every discovered delivery candidate intentionally excluded from the external CLI surface.

Excluded candidates: **13**

## [exclusion/process-launcher-not-cli/v1](../policies/index.md#p-572523784c3c)

The installed entry point starts a separately inventoried process protocol and does not expose an independently maintained human or JSON CLI.

### Excluded candidates

- <a id="x-63f77f282f46"></a>`excluded:console-script:riverhog-api`
  - kind: `console-script`
  - installed target: `riverhog_api.app:main`
- <a id="x-ee73027540c0"></a>`excluded:console-script:riverhog-storage-adapter-aws`
  - kind: `console-script`
  - installed target: `riverhog_storage_adapter_aws.app:main`
- <a id="x-be2c1e6a3c99"></a>`excluded:console-script:riverhog-storage-adapter-backblaze`
  - kind: `console-script`
  - installed target: `riverhog_storage_adapter_backblaze.app:main`
- <a id="x-33b1e7c70687"></a>`excluded:console-script:riverhog-storage-adapter-filesystem`
  - kind: `console-script`
  - installed target: `riverhog_storage_adapter_filesystem.app:main`
- <a id="x-19aaa60bc835"></a>`excluded:console-script:stove0-exiftool-observer`
  - kind: `console-script`
  - installed target: `stove0_exiftool_observer.app:main`
- <a id="x-8dc5aa88efcd"></a>`excluded:console-script:stove0-ffprobe-sampling-observer`
  - kind: `console-script`
  - installed target: `stove0_ffprobe_sampling_observer.app:main`
- <a id="x-260ce86f87ac"></a>`excluded:console-script:stove0-nvenc-av1-opus-review-sampler`
  - kind: `console-script`
  - installed target: `stove0_nvenc_av1_opus_review_sampler.app:main`
- <a id="x-e3f6f904097d"></a>`excluded:console-script:stove0-nvenc-av1-opus-target`
  - kind: `console-script`
  - installed target: `stove0_nvenc_av1_opus_target.app:target_main`
- <a id="x-0059b6826946"></a>`excluded:console-script:stove0-opus-review-sampler`
  - kind: `console-script`
  - installed target: `stove0_opus_review_sampler.app:main`
- <a id="x-f6baa8fe31d4"></a>`excluded:console-script:stove0-opus-target`
  - kind: `console-script`
  - installed target: `stove0_opus_target.app:target_main`
- <a id="x-fb6964baf679"></a>`excluded:console-script:stove0-review-materialize-target`
  - kind: `console-script`
  - installed target: `stove0_review_materialize_target.app:main`
- <a id="x-3d62f695c1f7"></a>`excluded:console-script:stove0-review-rclone-effect-target`
  - kind: `console-script`
  - installed target: `stove0_review_rclone_effect_target.app:main`
- <a id="x-0a4320a594dd"></a>`excluded:console-script:stove0-server`
  - kind: `console-script`
  - installed target: `stove0_api.app:main`

### Exact accounting

| Candidate | Boundary source | Detector | Source authority |
|---|---|---|---|
| [excluded:console-script:riverhog-api](#x-63f77f282f46) | `/boundaries/components/70/console_scripts/riverhog-api` | `cli-tree` | [release:release.toml](sources.md#src-c5380dbe5fe0) |
| [excluded:console-script:riverhog-storage-adapter-aws](#x-ee73027540c0) | `/boundaries/components/36/console_scripts/riverhog-storage-adapter-aws` | `cli-tree` | [release:release.toml](sources.md#src-c5380dbe5fe0) |
| [excluded:console-script:riverhog-storage-adapter-backblaze](#x-be2c1e6a3c99) | `/boundaries/components/37/console_scripts/riverhog-storage-adapter-backblaze` | `cli-tree` | [release:release.toml](sources.md#src-c5380dbe5fe0) |
| [excluded:console-script:riverhog-storage-adapter-filesystem](#x-33b1e7c70687) | `/boundaries/components/38/console_scripts/riverhog-storage-adapter-filesystem` | `cli-tree` | [release:release.toml](sources.md#src-c5380dbe5fe0) |
| [excluded:console-script:stove0-exiftool-observer](#x-19aaa60bc835) | `/boundaries/components/44/console_scripts/stove0-exiftool-observer` | `cli-tree` | [release:release.toml](sources.md#src-c5380dbe5fe0) |
| [excluded:console-script:stove0-ffprobe-sampling-observer](#x-8dc5aa88efcd) | `/boundaries/components/45/console_scripts/stove0-ffprobe-sampling-observer` | `cli-tree` | [release:release.toml](sources.md#src-c5380dbe5fe0) |
| [excluded:console-script:stove0-nvenc-av1-opus-review-sampler](#x-260ce86f87ac) | `/boundaries/components/58/console_scripts/stove0-nvenc-av1-opus-review-sampler` | `cli-tree` | [release:release.toml](sources.md#src-c5380dbe5fe0) |
| [excluded:console-script:stove0-nvenc-av1-opus-target](#x-e3f6f904097d) | `/boundaries/components/59/console_scripts/stove0-nvenc-av1-opus-target` | `cli-tree` | [release:release.toml](sources.md#src-c5380dbe5fe0) |
| [excluded:console-script:stove0-opus-review-sampler](#x-0059b6826946) | `/boundaries/components/60/console_scripts/stove0-opus-review-sampler` | `cli-tree` | [release:release.toml](sources.md#src-c5380dbe5fe0) |
| [excluded:console-script:stove0-opus-target](#x-f6baa8fe31d4) | `/boundaries/components/61/console_scripts/stove0-opus-target` | `cli-tree` | [release:release.toml](sources.md#src-c5380dbe5fe0) |
| [excluded:console-script:stove0-review-materialize-target](#x-fb6964baf679) | `/boundaries/components/63/console_scripts/stove0-review-materialize-target` | `cli-tree` | [release:release.toml](sources.md#src-c5380dbe5fe0) |
| [excluded:console-script:stove0-review-rclone-effect-target](#x-3d62f695c1f7) | `/boundaries/components/65/console_scripts/stove0-review-rclone-effect-target` | `cli-tree` | [release:release.toml](sources.md#src-c5380dbe5fe0) |
| [excluded:console-script:stove0-server](#x-0a4320a594dd) | `/boundaries/components/41/console_scripts/stove0-server` | `cli-tree` | [release:release.toml](sources.md#src-c5380dbe5fe0) |
