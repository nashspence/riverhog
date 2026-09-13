# Runtime image: riverhog-storage-adapter-filesystem

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:runtime-image-riverhog-storage-adapter-filesystem:7a23fa9d06 -->

Optional nonnormative Linux filesystem storage reference for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-85a35d7977"></a>
| Concern | Contract |
|---|---|
| <a id="s-e2299de9b7"></a>`build_target` | riverhog-storage-adapter-filesystem |
| <a id="s-f1520cc27d"></a>`description` | Optional nonnormative Linux filesystem storage reference for Riverhog. |
| <a id="s-f627f0712c"></a>`distribution_roots` | ["riverhog-storage-adapter-filesystem"] |
| <a id="s-2f65603f9b"></a>`format` | oci-image |
| <a id="s-9eddfa0176"></a>`platforms` | ["linux/amd64"] |
| <a id="s-150ca33870"></a>`repository` | ghcr.io/nashspence/riverhog-storage-adapter-filesystem |
| <a id="s-0c7b9069ce"></a>`role` | reference |
| <a id="s-6418eb9e41"></a>`tag_templates` | ["ghcr.io/nashspence/riverhog-storage-adapter-filesystem:{version}", "ghcr.io/nashspence/riverhog-storage-adapter-filesystem:sha-{source_sha}"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-filesystem](../../../evidence/relationships.md#rn-ebe4206627)

## Governing policies

- <a id="pa-d0b670aa11"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

- `/external_contract/release/publication/runtime_images/riverhog-storage-adapter-filesystem`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fea6891b5e39874e794d53db75c198ae74c987f48af89fddc2c450faa2139cb7 -->

```json
{
  "build_target": "riverhog-storage-adapter-filesystem",
  "description": "Optional nonnormative Linux filesystem storage reference for Riverhog.",
  "distribution_roots": [
    "riverhog-storage-adapter-filesystem"
  ],
  "format": "oci-image",
  "platforms": [
    "linux/amd64"
  ],
  "repository": "ghcr.io/nashspence/riverhog-storage-adapter-filesystem",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-storage-adapter-filesystem:{version}",
    "ghcr.io/nashspence/riverhog-storage-adapter-filesystem:sha-{source_sha}"
  ]
}
```
