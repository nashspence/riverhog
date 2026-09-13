# Runtime image: stove0-review-materialize-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:runtime-image-stove0-review-materialize-target:190bd176f8 -->

Optional nonnormative review materialization target reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-b324a3073f"></a>
| Concern | Contract |
|---|---|
| <a id="s-3e86e50c59"></a>`build_target` | stove0-review-materialize-target |
| <a id="s-d8a98ea2e9"></a>`description` | Optional nonnormative review materialization target reference for Stove0. |
| <a id="s-d2b0aeac6c"></a>`distribution_roots` | ["stove0-review-materialize-target"] |
| <a id="s-4bffbf8ede"></a>`format` | oci-image |
| <a id="s-db50faa7bd"></a>`platforms` | ["linux/amd64"] |
| <a id="s-a595fd7a97"></a>`repository` | ghcr.io/nashspence/riverhog-stove0-review-materialize-target |
| <a id="s-28bbf4cfa9"></a>`role` | reference |
| <a id="s-81159459cd"></a>`tag_templates` | ["ghcr.io/nashspence/riverhog-stove0-review-materialize-target:{version}", "ghcr.io/nashspence/riverhog-stove0-review-materialize-target:sha-{source_sha}"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-materialize-target](../../../evidence/relationships.md#rn-4086d947a3)

## Governing policies

- <a id="pa-751a292b61"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

- `/external_contract/release/publication/runtime_images/stove0-review-materialize-target`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce7d9fec2bb5b9e2034eec6dd6a489b4b2819976838271222fec8da130ec5980 -->

```json
{
  "build_target": "stove0-review-materialize-target",
  "description": "Optional nonnormative review materialization target reference for Stove0.",
  "distribution_roots": [
    "stove0-review-materialize-target"
  ],
  "format": "oci-image",
  "platforms": [
    "linux/amd64"
  ],
  "repository": "ghcr.io/nashspence/riverhog-stove0-review-materialize-target",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-stove0-review-materialize-target:{version}",
    "ghcr.io/nashspence/riverhog-stove0-review-materialize-target:sha-{source_sha}"
  ]
}
```
