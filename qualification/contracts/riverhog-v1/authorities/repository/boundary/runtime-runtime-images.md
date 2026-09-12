# Runtime runtime images

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:repository:runtime-runtime-images:27155f9290 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [repository](../index.md) |
| Interface | [boundary](index.md) |
| Family | [runtime-images](index.md#f-dfd50c0c86) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-17a1a46b61"></a>
| Field | Shape |
|---|---|
| <a id="s-2952bbdb12"></a>`mango-fish` | additional keys=`distributions`, `repository`, `role` |
| <a id="s-9315017c63"></a>`riverhog` | additional keys=`distributions`, `repository`, `role` |
| <a id="s-b49d3cef94"></a>`riverhog-ftp-adapter` | additional keys=`distributions`, `repository`, `role` |
| <a id="s-7f41767266"></a>`riverhog-storage-adapter-aws` | additional keys=`distributions`, `repository`, `role` |
| <a id="s-d2d511eb97"></a>`riverhog-storage-adapter-backblaze` | additional keys=`distributions`, `repository`, `role` |
| <a id="s-3fab272277"></a>`riverhog-storage-adapter-filesystem` | additional keys=`distributions`, `repository`, `role` |
| <a id="s-1b663a494b"></a>`stove0` | additional keys=`distributions`, `repository`, `role` |
| <a id="s-726b3e0083"></a>`stove0-exiftool-observer` | additional keys=`distributions`, `repository`, `role` |
| <a id="s-8ab83ad691"></a>`stove0-ffprobe-sampling-observer` | additional keys=`distributions`, `repository`, `role` |
| <a id="s-98935a81be"></a>`stove0-nvenc-av1-opus-target` | additional keys=`distributions`, `repository`, `role` |
| <a id="s-53e94fd89e"></a>`stove0-opus-target` | additional keys=`distributions`, `repository`, `role` |
| <a id="s-62e71858cb"></a>`stove0-review-materialize-target` | additional keys=`distributions`, `repository`, `role` |
| <a id="s-dce858de27"></a>`stove0-review-rclone-effect-target` | additional keys=`distributions`, `repository`, `role` |

## Governing policies

- <a id="pa-ac266ccb83"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/runtime_images/runtime`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c6f43774c53a9c18841f23500fa652a943887a34c636f309f5f34496e687171f -->

```json
{
  "mango-fish": {
    "description": "Optional nonnormative CloudEvents reference application for Riverhog.",
    "distributions": [
      "mango-fish"
    ],
    "repository": "ghcr.io/nashspence/riverhog-mango-fish",
    "role": "reference"
  },
  "riverhog": {
    "description": "Riverhog archive service.",
    "distributions": [
      "riverhog-server"
    ],
    "repository": "ghcr.io/nashspence/riverhog",
    "role": "product"
  },
  "riverhog-ftp-adapter": {
    "description": "Optional nonnormative Riverhog FTP ingress reference.",
    "distributions": [
      "riverhog-ftp-adapter",
      "riverhog-provenance-linux-observer"
    ],
    "repository": "ghcr.io/nashspence/riverhog-ftp-adapter",
    "role": "reference"
  },
  "riverhog-storage-adapter-aws": {
    "description": "Optional nonnormative AWS storage reference for Riverhog.",
    "distributions": [
      "riverhog-storage-adapter-aws"
    ],
    "repository": "ghcr.io/nashspence/riverhog-storage-adapter-aws",
    "role": "reference"
  },
  "riverhog-storage-adapter-backblaze": {
    "description": "Optional nonnormative Backblaze B2 storage reference for Riverhog.",
    "distributions": [
      "riverhog-storage-adapter-backblaze"
    ],
    "repository": "ghcr.io/nashspence/riverhog-storage-adapter-backblaze",
    "role": "reference"
  },
  "riverhog-storage-adapter-filesystem": {
    "description": "Optional nonnormative Linux filesystem storage reference for Riverhog.",
    "distributions": [
      "riverhog-storage-adapter-filesystem"
    ],
    "repository": "ghcr.io/nashspence/riverhog-storage-adapter-filesystem",
    "role": "reference"
  },
  "stove0": {
    "description": "Optional nonnormative transformation reference application for Riverhog.",
    "distributions": [
      "stove0-server"
    ],
    "repository": "ghcr.io/nashspence/riverhog-stove0",
    "role": "reference"
  },
  "stove0-exiftool-observer": {
    "description": "Optional nonnormative ExifTool observer reference for Stove0.",
    "distributions": [
      "stove0-exiftool-observer"
    ],
    "repository": "ghcr.io/nashspence/riverhog-stove0-exiftool-observer",
    "role": "reference"
  },
  "stove0-ffprobe-sampling-observer": {
    "description": "Optional nonnormative FFprobe sampling-observer reference for Stove0.",
    "distributions": [
      "stove0-ffprobe-sampling-observer"
    ],
    "repository": "ghcr.io/nashspence/riverhog-stove0-ffprobe-sampling-observer",
    "role": "reference"
  },
  "stove0-nvenc-av1-opus-target": {
    "description": "Optional nonnormative NVENC AV1 and Opus target reference for Stove0.",
    "distributions": [
      "stove0-nvenc-av1-opus-target",
      "stove0-nvenc-av1-opus-review-sampler"
    ],
    "repository": "ghcr.io/nashspence/riverhog-stove0-nvenc-av1-opus-target",
    "role": "reference"
  },
  "stove0-opus-target": {
    "description": "Optional nonnormative Opus target reference for Stove0.",
    "distributions": [
      "stove0-opus-target",
      "stove0-opus-review-sampler"
    ],
    "repository": "ghcr.io/nashspence/riverhog-stove0-opus-target",
    "role": "reference"
  },
  "stove0-review-materialize-target": {
    "description": "Optional nonnormative review materialization target reference for Stove0.",
    "distributions": [
      "stove0-review-materialize-target"
    ],
    "repository": "ghcr.io/nashspence/riverhog-stove0-review-materialize-target",
    "role": "reference"
  },
  "stove0-review-rclone-effect-target": {
    "description": "Optional nonnormative rclone review-effect target reference for Stove0.",
    "distributions": [
      "stove0-review-rclone-effect-target"
    ],
    "repository": "ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target",
    "role": "reference"
  }
}
```
