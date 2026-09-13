# Runtime image: stove0-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-stove0-opus-target:3a05a8d0f4 -->

Optional nonnormative Opus target reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-cb18d0661e"></a>
| Concern | Contract |
|---|---|
| <a id="s-c0a4ab77dc"></a>`build_target` | stove0-opus-target |
| <a id="s-d92b0b7680"></a>`description` | Optional nonnormative Opus target reference for Stove0. |
| <a id="s-6f67b15332"></a>`distribution_roots` | ["stove0-opus-target", "stove0-opus-review-sampler"] |
| <a id="s-627b5e2cdc"></a>`format` | oci-image |
| <a id="s-ea76c70402"></a>`platforms` | ["linux/amd64"] |
| <a id="s-65067a059b"></a>`repository` | ghcr.io/nashspence/riverhog-stove0-opus-target |
| <a id="s-6b9062690b"></a>`role` | reference |
| <a id="s-20860bcd0d"></a>`tag_templates` | ["ghcr.io/nashspence/riverhog-stove0-opus-target:{version}", "ghcr.io/nashspence/riverhog-stove0-opus-target:sha-{source_sha}"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-opus-target](../../../evidence/relationships.md#rn-313a5c450f)
- [stove0-opus-review-sampler](../../../evidence/relationships.md#rn-36756ccd77)

## Governing policies

- <a id="pa-7484c74640"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-851ed1a119"></a>[publication/image-digest-scope/v1](../../../policies/index.md#p-634e69c23f)
- <a id="pa-c2d97470b8"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-0bf3ec2364"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/stove0-opus-target`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5dbe2c187f72105aa4d332978d609396be7f48bea2ba8197acd98da0920c2d43 -->

```json
{
  "build_target": "stove0-opus-target",
  "description": "Optional nonnormative Opus target reference for Stove0.",
  "distribution_roots": [
    "stove0-opus-target",
    "stove0-opus-review-sampler"
  ],
  "format": "oci-image",
  "platforms": [
    "linux/amd64"
  ],
  "repository": "ghcr.io/nashspence/riverhog-stove0-opus-target",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-stove0-opus-target:{version}",
    "ghcr.io/nashspence/riverhog-stove0-opus-target:sha-{source_sha}"
  ]
}
```
