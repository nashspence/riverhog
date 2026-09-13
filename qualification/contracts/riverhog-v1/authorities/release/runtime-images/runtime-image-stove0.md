# Runtime image: stove0

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-stove0:100d9c8df0 -->

Optional nonnormative transformation reference application for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-cf3fdaa31c"></a>
| Concern | Contract |
|---|---|
| <a id="s-a66428c82b"></a>`build_target` | stove0 |
| <a id="s-4539b9404a"></a>`description` | Optional nonnormative transformation reference application for Riverhog. |
| <a id="s-b9fa096e27"></a>`distribution_roots` | ["stove0-server"] |
| <a id="s-3387ab3766"></a>`format` | oci-image |
| <a id="s-858594d9f9"></a>`platforms` | ["linux/amd64"] |
| <a id="s-27218090de"></a>`repository` | ghcr.io/nashspence/riverhog-stove0 |
| <a id="s-309b0af8a3"></a>`role` | reference |
| <a id="s-1ca6b0b8f3"></a>`tag_templates` | ["ghcr.io/nashspence/riverhog-stove0:{version}", "ghcr.io/nashspence/riverhog-stove0:sha-{source_sha}"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-server](../../../evidence/relationships.md#rn-3540de4d4a)

## Governing policies

- <a id="pa-677a186656"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-07071b766b"></a>[publication/image-digest-scope/v1](../../../policies/index.md#p-634e69c23f)
- <a id="pa-4340a1c177"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-ee83714149"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/stove0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2941307853659172c44fbb16d2e7d4b8eef5763d9bb8da0bdb3d68434b5a5cc7 -->

```json
{
  "build_target": "stove0",
  "description": "Optional nonnormative transformation reference application for Riverhog.",
  "distribution_roots": [
    "stove0-server"
  ],
  "format": "oci-image",
  "platforms": [
    "linux/amd64"
  ],
  "repository": "ghcr.io/nashspence/riverhog-stove0",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-stove0:{version}",
    "ghcr.io/nashspence/riverhog-stove0:sha-{source_sha}"
  ]
}
```
