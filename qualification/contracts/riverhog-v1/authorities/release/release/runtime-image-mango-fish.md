# Runtime image: mango-fish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:runtime-image-mango-fish:916cd1d4d5 -->

Optional nonnormative CloudEvents reference application for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-2ef14eb88e"></a>
| Concern | Contract |
|---|---|
| <a id="s-1974608c7c"></a>`build_target` | mango-fish |
| <a id="s-dd1674b6ff"></a>`description` | Optional nonnormative CloudEvents reference application for Riverhog. |
| <a id="s-8f4dce0db3"></a>`distribution_roots` | ["mango-fish"] |
| <a id="s-c131e3d0f1"></a>`format` | oci-image |
| <a id="s-8ce94d6fbc"></a>`platforms` | ["linux/amd64"] |
| <a id="s-62b24a5072"></a>`repository` | ghcr.io/nashspence/riverhog-mango-fish |
| <a id="s-0453acaaea"></a>`role` | reference |
| <a id="s-0c5f1f0ff3"></a>`tag_templates` | ["ghcr.io/nashspence/riverhog-mango-fish:{version}", "ghcr.io/nashspence/riverhog-mango-fish:sha-{source_sha}"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [mango-fish](../../../evidence/relationships.md#rn-1c3900c994)

## Governing policies

- <a id="pa-457201dfee"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

- `/external_contract/release/publication/runtime_images/mango-fish`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14bedf6f00b7aa39432b98070919d0a68766e2394f8fef3b85fdee52e577a527 -->

```json
{
  "build_target": "mango-fish",
  "description": "Optional nonnormative CloudEvents reference application for Riverhog.",
  "distribution_roots": [
    "mango-fish"
  ],
  "format": "oci-image",
  "platforms": [
    "linux/amd64"
  ],
  "repository": "ghcr.io/nashspence/riverhog-mango-fish",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-mango-fish:{version}",
    "ghcr.io/nashspence/riverhog-mango-fish:sha-{source_sha}"
  ]
}
```
