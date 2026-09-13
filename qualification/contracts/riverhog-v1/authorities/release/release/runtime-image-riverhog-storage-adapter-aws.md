# Runtime image: riverhog-storage-adapter-aws

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:runtime-image-riverhog-storage-adapter-aws:bc5f0aeb4b -->

Optional nonnormative AWS storage reference for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-d90ca4ab11"></a>
| Concern | Contract |
|---|---|
| <a id="s-80cb157b52"></a>`build_target` | riverhog-storage-adapter-aws |
| <a id="s-2be3a6ebff"></a>`description` | Optional nonnormative AWS storage reference for Riverhog. |
| <a id="s-471d6d0a25"></a>`distribution_roots` | ["riverhog-storage-adapter-aws"] |
| <a id="s-37c9a8f589"></a>`format` | oci-image |
| <a id="s-6c4435dc28"></a>`platforms` | ["linux/amd64"] |
| <a id="s-2d2af6f16d"></a>`repository` | ghcr.io/nashspence/riverhog-storage-adapter-aws |
| <a id="s-5558a4421f"></a>`role` | reference |
| <a id="s-3342698ea1"></a>`tag_templates` | ["ghcr.io/nashspence/riverhog-storage-adapter-aws:{version}", "ghcr.io/nashspence/riverhog-storage-adapter-aws:sha-{source_sha}"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-aws](../../../evidence/relationships.md#rn-c53f4ff3ef)

## Governing policies

- <a id="pa-07570873bb"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

- `/external_contract/release/publication/runtime_images/riverhog-storage-adapter-aws`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f9125ecb73b3532aa202f2f028ceb4090e89d60254d215d5f71f0c531183261 -->

```json
{
  "build_target": "riverhog-storage-adapter-aws",
  "description": "Optional nonnormative AWS storage reference for Riverhog.",
  "distribution_roots": [
    "riverhog-storage-adapter-aws"
  ],
  "format": "oci-image",
  "platforms": [
    "linux/amd64"
  ],
  "repository": "ghcr.io/nashspence/riverhog-storage-adapter-aws",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-storage-adapter-aws:{version}",
    "ghcr.io/nashspence/riverhog-storage-adapter-aws:sha-{source_sha}"
  ]
}
```
