# Explicitly excluded candidates

[Atlas](../index.md) · [Freeze evidence](index.md) · [Policies](../policies/index.md)

This page supports the ‘no more’ side of the audit by naming every discovered delivery candidate intentionally excluded from the external CLI surface.

Excluded candidates: **13**

## `exclusion/process-launcher-not-cli/v1`

The installed entry point starts a separately inventoried process protocol and does not expose an independently maintained human or JSON CLI.

### Excluded candidates

- `excluded:console-script:riverhog-api`
  - kind: `console-script`
  - installed target: `riverhog_api.app:main`
- `excluded:console-script:riverhog-storage-adapter-aws`
  - kind: `console-script`
  - installed target: `riverhog_storage_adapter_aws.app:main`
- `excluded:console-script:riverhog-storage-adapter-backblaze`
  - kind: `console-script`
  - installed target: `riverhog_storage_adapter_backblaze.app:main`
- `excluded:console-script:riverhog-storage-adapter-filesystem`
  - kind: `console-script`
  - installed target: `riverhog_storage_adapter_filesystem.app:main`
- `excluded:console-script:stove0-exiftool-observer`
  - kind: `console-script`
  - installed target: `stove0_exiftool_observer.app:main`
- `excluded:console-script:stove0-ffprobe-sampling-observer`
  - kind: `console-script`
  - installed target: `stove0_ffprobe_sampling_observer.app:main`
- `excluded:console-script:stove0-nvenc-av1-opus-review-sampler`
  - kind: `console-script`
  - installed target: `stove0_nvenc_av1_opus_review_sampler.app:main`
- `excluded:console-script:stove0-nvenc-av1-opus-target`
  - kind: `console-script`
  - installed target: `stove0_nvenc_av1_opus_target.app:target_main`
- `excluded:console-script:stove0-opus-review-sampler`
  - kind: `console-script`
  - installed target: `stove0_opus_review_sampler.app:main`
- `excluded:console-script:stove0-opus-target`
  - kind: `console-script`
  - installed target: `stove0_opus_target.app:target_main`
- `excluded:console-script:stove0-review-materialize-target`
  - kind: `console-script`
  - installed target: `stove0_review_materialize_target.app:main`
- `excluded:console-script:stove0-review-rclone-effect-target`
  - kind: `console-script`
  - installed target: `stove0_review_rclone_effect_target.app:main`
- `excluded:console-script:stove0-server`
  - kind: `console-script`
  - installed target: `stove0_api.app:main`

### Exact accounting

| Candidate | Boundary source | Detector | Source authority |
|---|---|---|---|
| `excluded:console-script:riverhog-api` | `/boundaries/components/70/console_scripts/riverhog-api` | `cli-tree` | `release:release.toml` |
| `excluded:console-script:riverhog-storage-adapter-aws` | `/boundaries/components/36/console_scripts/riverhog-storage-adapter-aws` | `cli-tree` | `release:release.toml` |
| `excluded:console-script:riverhog-storage-adapter-backblaze` | `/boundaries/components/37/console_scripts/riverhog-storage-adapter-backblaze` | `cli-tree` | `release:release.toml` |
| `excluded:console-script:riverhog-storage-adapter-filesystem` | `/boundaries/components/38/console_scripts/riverhog-storage-adapter-filesystem` | `cli-tree` | `release:release.toml` |
| `excluded:console-script:stove0-exiftool-observer` | `/boundaries/components/44/console_scripts/stove0-exiftool-observer` | `cli-tree` | `release:release.toml` |
| `excluded:console-script:stove0-ffprobe-sampling-observer` | `/boundaries/components/45/console_scripts/stove0-ffprobe-sampling-observer` | `cli-tree` | `release:release.toml` |
| `excluded:console-script:stove0-nvenc-av1-opus-review-sampler` | `/boundaries/components/58/console_scripts/stove0-nvenc-av1-opus-review-sampler` | `cli-tree` | `release:release.toml` |
| `excluded:console-script:stove0-nvenc-av1-opus-target` | `/boundaries/components/59/console_scripts/stove0-nvenc-av1-opus-target` | `cli-tree` | `release:release.toml` |
| `excluded:console-script:stove0-opus-review-sampler` | `/boundaries/components/60/console_scripts/stove0-opus-review-sampler` | `cli-tree` | `release:release.toml` |
| `excluded:console-script:stove0-opus-target` | `/boundaries/components/61/console_scripts/stove0-opus-target` | `cli-tree` | `release:release.toml` |
| `excluded:console-script:stove0-review-materialize-target` | `/boundaries/components/63/console_scripts/stove0-review-materialize-target` | `cli-tree` | `release:release.toml` |
| `excluded:console-script:stove0-review-rclone-effect-target` | `/boundaries/components/65/console_scripts/stove0-review-rclone-effect-target` | `cli-tree` | `release:release.toml` |
| `excluded:console-script:stove0-server` | `/boundaries/components/41/console_scripts/stove0-server` | `cli-tree` | `release:release.toml` |
