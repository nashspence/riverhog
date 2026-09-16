# Runtime image: stove0-review-rclone-effect-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-stove0-review-rclone-effect-target:464487a491 -->

Optional nonnormative rclone review-effect target reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-40706db2a4"></a>
| Concern | Contract |
|---|---|
| <a id="s-481632c28f"></a>`build_target` | `"stove0-review-rclone-effect-target"` |
| <a id="s-91bbb20e16"></a>`description` | `"Optional nonnormative rclone review-effect target reference for Stove0."` |
| <a id="s-7c5361065b"></a>`distribution_roots` | `["stove0-review-rclone-effect-target"]` |
| <a id="s-cd35f7db3e"></a>`format` | `"oci-image"` |
| <a id="s-568af04885"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-a0f1429833"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-c112e79293"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-67797b286a"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target","kind":"oci-repository"}` |
| <a id="s-02d8536797"></a>`repository` | `"ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target"` |
| <a id="s-d1c90db606"></a>`role` | `"reference"` |
| <a id="s-b9fd80844d"></a>`tag_templates` | `["ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target:{version}","ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-rclone-effect-target](../../../evidence/relationships.md#rn-87283d63df)

## Governing policies

- <a id="pa-3aeb1de6a0"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-039fd13c46"></a>[publication/image-digest-scope/v1](../../../policies/index.md#p-634e69c23f)
- <a id="pa-4f4b594a85"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-d3d8f02ca9"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-images:docker-bake](../../../evidence/sources.md#src-8d3f4df21c) — `docker-bake.hcl`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/runtime_images/stove0-review-rclone-effect-target`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4a2c5271a31cde1772ea827628bd89675a6354d674821240c39e5552447f153f -->

```json
{
  "build_target": "stove0-review-rclone-effect-target",
  "description": "Optional nonnormative rclone review-effect target reference for Stove0.",
  "distribution_roots": [
    "stove0-review-rclone-effect-target"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target:{version}",
    "ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target:sha-{source_sha}"
  ]
}
```

</details>
