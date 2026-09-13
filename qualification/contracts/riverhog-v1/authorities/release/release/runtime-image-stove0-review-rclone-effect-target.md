# Runtime image: stove0-review-rclone-effect-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:runtime-image-stove0-review-rclone-effect-target:17bfc14dfc -->

Optional nonnormative rclone review-effect target reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-40706db2a4"></a>
| Concern | Contract |
|---|---|
| <a id="s-481632c28f"></a>`build_target` | stove0-review-rclone-effect-target |
| <a id="s-91bbb20e16"></a>`description` | Optional nonnormative rclone review-effect target reference for Stove0. |
| <a id="s-7c5361065b"></a>`distribution_roots` | ["stove0-review-rclone-effect-target"] |
| <a id="s-cd35f7db3e"></a>`format` | oci-image |
| <a id="s-c112e79293"></a>`platforms` | ["linux/amd64"] |
| <a id="s-02d8536797"></a>`repository` | ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target |
| <a id="s-d1c90db606"></a>`role` | reference |
| <a id="s-b9fd80844d"></a>`tag_templates` | ["ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target:{version}", "ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target:sha-{source_sha}"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-rclone-effect-target](../../../evidence/relationships.md#rn-87283d63df)

## Governing policies

- <a id="pa-19be1b4eb8"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0ab570f6304609a12bc7921006f38cbbc2b4af10887590a0bb9a4a46c27a7f6 -->

```json
{
  "build_target": "stove0-review-rclone-effect-target",
  "description": "Optional nonnormative rclone review-effect target reference for Stove0.",
  "distribution_roots": [
    "stove0-review-rclone-effect-target"
  ],
  "format": "oci-image",
  "platforms": [
    "linux/amd64"
  ],
  "repository": "ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target:{version}",
    "ghcr.io/nashspence/riverhog-stove0-review-rclone-effect-target:sha-{source_sha}"
  ]
}
```
