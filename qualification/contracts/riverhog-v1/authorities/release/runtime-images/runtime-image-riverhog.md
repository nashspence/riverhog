# Runtime image: riverhog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-riverhog:e6d74c899b -->

Riverhog archive service.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-db4835f937"></a>
| Concern | Contract |
|---|---|
| <a id="s-6b3d019894"></a>`build_target` | riverhog |
| <a id="s-9ea71fa0fc"></a>`description` | Riverhog archive service. |
| <a id="s-192ec51a07"></a>`distribution_roots` | ["riverhog-server"] |
| <a id="s-49a516eccb"></a>`format` | oci-image |
| <a id="s-6f9a9d224b"></a>`license_baseline` | first-v1-publication |
| <a id="s-e9ce023134"></a>`license_expression` | CAL-1.0 |
| <a id="s-6be1951258"></a>`platforms` | ["linux/amd64"] |
| <a id="s-2356f94695"></a>`publication_identity` | {"coordinate": "ghcr.io/nashspence/riverhog", "kind": "oci-repository"} |
| <a id="s-7e50a61d7f"></a>`repository` | ghcr.io/nashspence/riverhog |
| <a id="s-69132026d3"></a>`role` | product |
| <a id="s-aae0d7766d"></a>`tag_templates` | ["ghcr.io/nashspence/riverhog:{version}", "ghcr.io/nashspence/riverhog:sha-{source_sha}"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-server](../../../evidence/relationships.md#rn-807c63322e)

## Governing policies

- <a id="pa-181d9e45b3"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-78ac8dcbdc"></a>[publication/image-digest-scope/v1](../../../policies/index.md#p-634e69c23f)
- <a id="pa-c5ad4f5696"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-588ca4eadf"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/riverhog`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f47076225f65b55347f4b047edf574f283bb08cbc4697526d53bc26c2b68e2a9 -->

```json
{
  "build_target": "riverhog",
  "description": "Riverhog archive service.",
  "distribution_roots": [
    "riverhog-server"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/riverhog",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/riverhog",
  "role": "product",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog:{version}",
    "ghcr.io/nashspence/riverhog:sha-{source_sha}"
  ]
}
```
