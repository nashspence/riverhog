# Runtime image: mango-fish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-mango-fish:9c6cce1bdf -->

Optional nonnormative CloudEvents reference application for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-2ef14eb88e"></a>
| Concern | Contract |
|---|---|
| <a id="s-1974608c7c"></a>`build_target` | mango-fish |
| <a id="s-dd1674b6ff"></a>`description` | Optional nonnormative CloudEvents reference application for Riverhog. |
| <a id="s-8f4dce0db3"></a>`distribution_roots` | ["mango-fish"] |
| <a id="s-c131e3d0f1"></a>`format` | oci-image |
| <a id="s-b9601b6fad"></a>`license_baseline` | first-v1-publication |
| <a id="s-dee9ac5a1b"></a>`license_expression` | Apache-2.0 |
| <a id="s-8ce94d6fbc"></a>`platforms` | ["linux/amd64"] |
| <a id="s-992c81a6da"></a>`publication_identity` | {"coordinate": "ghcr.io/nashspence/riverhog-mango-fish", "kind": "oci-repository"} |
| <a id="s-62b24a5072"></a>`repository` | ghcr.io/nashspence/riverhog-mango-fish |
| <a id="s-0453acaaea"></a>`role` | reference |
| <a id="s-0c5f1f0ff3"></a>`tag_templates` | ["ghcr.io/nashspence/riverhog-mango-fish:{version}", "ghcr.io/nashspence/riverhog-mango-fish:sha-{source_sha}"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [mango-fish](../../../evidence/relationships.md#rn-1c3900c994)

## Governing policies

- <a id="pa-548cbf5f12"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-3df8a8d352"></a>[publication/image-digest-scope/v1](../../../policies/index.md#p-634e69c23f)
- <a id="pa-6654233767"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-f379800e67"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

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

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13e9b1214d609ac72d4e61fda500cf51f30ef459901a322a81a519c70531ec22 -->

```json
{
  "build_target": "mango-fish",
  "description": "Optional nonnormative CloudEvents reference application for Riverhog.",
  "distribution_roots": [
    "mango-fish"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/riverhog-mango-fish",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/riverhog-mango-fish",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-mango-fish:{version}",
    "ghcr.io/nashspence/riverhog-mango-fish:sha-{source_sha}"
  ]
}
```

</details>
