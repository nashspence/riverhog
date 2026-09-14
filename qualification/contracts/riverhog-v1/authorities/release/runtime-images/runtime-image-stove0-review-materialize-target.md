# Runtime image: stove0-review-materialize-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-stove0-review-materialize-target:fe460853dd -->

Optional nonnormative review materialization target reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-b324a3073f"></a>
| Concern | Contract |
|---|---|
| <a id="s-3e86e50c59"></a>`build_target` | stove0-review-materialize-target |
| <a id="s-d8a98ea2e9"></a>`description` | Optional nonnormative review materialization target reference for Stove0. |
| <a id="s-d2b0aeac6c"></a>`distribution_roots` | ["stove0-review-materialize-target"] |
| <a id="s-4bffbf8ede"></a>`format` | oci-image |
| <a id="s-42d3ef7d72"></a>`license_baseline` | first-v1-publication |
| <a id="s-3800157d58"></a>`license_expression` | CAL-1.0 |
| <a id="s-db50faa7bd"></a>`platforms` | ["linux/amd64"] |
| <a id="s-f9cac31d43"></a>`publication_identity` | {"coordinate": "ghcr.io/nashspence/riverhog-stove0-review-materialize-target", "kind": "oci-repository"} |
| <a id="s-a595fd7a97"></a>`repository` | ghcr.io/nashspence/riverhog-stove0-review-materialize-target |
| <a id="s-28bbf4cfa9"></a>`role` | reference |
| <a id="s-81159459cd"></a>`tag_templates` | ["ghcr.io/nashspence/riverhog-stove0-review-materialize-target:{version}", "ghcr.io/nashspence/riverhog-stove0-review-materialize-target:sha-{source_sha}"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-materialize-target](../../../evidence/relationships.md#rn-4086d947a3)

## Governing policies

- <a id="pa-c1fa8ed66b"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-3726b21385"></a>[publication/image-digest-scope/v1](../../../policies/index.md#p-634e69c23f)
- <a id="pa-718d0f2187"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-8dab654c1e"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

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

<!-- exact-contract-value: ce6116a965888b389fa41031b18db7a9b878259a414f49d0fe2997807018f969 -->

```json
{
  "build_target": "stove0-review-materialize-target",
  "description": "Optional nonnormative review materialization target reference for Stove0.",
  "distribution_roots": [
    "stove0-review-materialize-target"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/riverhog-stove0-review-materialize-target",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/riverhog-stove0-review-materialize-target",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-stove0-review-materialize-target:{version}",
    "ghcr.io/nashspence/riverhog-stove0-review-materialize-target:sha-{source_sha}"
  ]
}
```
