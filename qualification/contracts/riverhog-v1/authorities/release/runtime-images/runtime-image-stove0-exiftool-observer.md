# Runtime image: stove0-exiftool-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-stove0-exiftool-observer:497964b4a8 -->

Optional nonnormative ExifTool observer reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-393510d514"></a>
| Concern | Contract |
|---|---|
| <a id="s-155ca48459"></a>`build_target` | stove0-exiftool-observer |
| <a id="s-c15e6a8b1f"></a>`description` | Optional nonnormative ExifTool observer reference for Stove0. |
| <a id="s-7f03ad7fc5"></a>`distribution_roots` | ["stove0-exiftool-observer"] |
| <a id="s-0f99045a79"></a>`format` | oci-image |
| <a id="s-c5d3857e90"></a>`platforms` | ["linux/amd64"] |
| <a id="s-3e1e7448e3"></a>`repository` | ghcr.io/nashspence/riverhog-stove0-exiftool-observer |
| <a id="s-54002ba5c8"></a>`role` | reference |
| <a id="s-234f712d78"></a>`tag_templates` | ["ghcr.io/nashspence/riverhog-stove0-exiftool-observer:{version}", "ghcr.io/nashspence/riverhog-stove0-exiftool-observer:sha-{source_sha}"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-exiftool-observer](../../../evidence/relationships.md#rn-9da0956020)

## Governing policies

- <a id="pa-5371766870"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-94a4c136eb"></a>[publication/image-digest-scope/v1](../../../policies/index.md#p-634e69c23f)
- <a id="pa-68a5145ff2"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-6b40599292"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/stove0-exiftool-observer`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9c5bcc76485c5976cf8733c2325d4b50449e1ce65be8bc61e9aa7aa479723095 -->

```json
{
  "build_target": "stove0-exiftool-observer",
  "description": "Optional nonnormative ExifTool observer reference for Stove0.",
  "distribution_roots": [
    "stove0-exiftool-observer"
  ],
  "format": "oci-image",
  "platforms": [
    "linux/amd64"
  ],
  "repository": "ghcr.io/nashspence/riverhog-stove0-exiftool-observer",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-stove0-exiftool-observer:{version}",
    "ghcr.io/nashspence/riverhog-stove0-exiftool-observer:sha-{source_sha}"
  ]
}
```
