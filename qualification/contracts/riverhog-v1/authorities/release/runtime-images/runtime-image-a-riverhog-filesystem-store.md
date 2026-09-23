# Runtime image: a-riverhog-filesystem-store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-a-riverhog-filesystem-store:913c59bff5 -->

Filesystem-backed Riverhog archive and retrieval store.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-7e322a3709"></a>
| Concern | Contract |
|---|---|
| <a id="s-bf5704d9f5"></a>`build_target` | `"a-riverhog-filesystem-store"` |
| <a id="s-57a393b828"></a>`description` | `"Filesystem-backed Riverhog archive and retrieval store."` |
| <a id="s-dc7eb4e4cd"></a>`distribution_roots` | `["a-riverhog-filesystem-store"]` |
| <a id="s-5433c7e822"></a>`format` | `"oci-image"` |
| <a id="s-7060ce25dd"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-1ec0c7fd72"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-69cea7acf1"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-5ac025ed46"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/a-riverhog-filesystem-store","kind":"oci-repository"}` |
| <a id="s-81252db697"></a>`repository` | `"ghcr.io/nashspence/a-riverhog-filesystem-store"` |
| <a id="s-8ad183ce9b"></a>`role` | `"component"` |
| <a id="s-7643404343"></a>`tag_templates` | `["ghcr.io/nashspence/a-riverhog-filesystem-store:{version}","ghcr.io/nashspence/a-riverhog-filesystem-store:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-filesystem-store](../../../evidence/relationships/nodes.md#rn-b69efa5ec4)

## Governing policies

- <a id="pa-5c9f117247"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-8e3e38852b"></a>[publication/image-digest-scope/v1](../../../policies/publication-image-digest-scope-v1/index.md#p-634e69c23f)
- <a id="pa-b473b74fb6"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-dbe687ef81"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-images:docker-bake](../../../evidence/sources/authorities.md#src-8d3f4df21c) — [docker-bake.hcl](../../../../../../docker-bake.hcl)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/runtime_images/a-riverhog-filesystem-store`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 71ce37107b8c60662d6f911b621d6e0aeeb85f7c89e6985fc8d35504f6182619 -->

```json
{
  "build_target": "a-riverhog-filesystem-store",
  "description": "Filesystem-backed Riverhog archive and retrieval store.",
  "distribution_roots": [
    "a-riverhog-filesystem-store"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/a-riverhog-filesystem-store",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/a-riverhog-filesystem-store",
  "role": "component",
  "tag_templates": [
    "ghcr.io/nashspence/a-riverhog-filesystem-store:{version}",
    "ghcr.io/nashspence/a-riverhog-filesystem-store:sha-{source_sha}"
  ]
}
```

</details>
