# Runtime image: a-riverhog-b2-store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-a-riverhog-b2-store:15b5d8ffdf -->

Backblaze B2-backed Riverhog archive and retrieval store.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-924c4e263a"></a>
| Concern | Contract |
|---|---|
| <a id="s-524509f123"></a>`build_target` | `"a-riverhog-b2-store"` |
| <a id="s-46d5c30ed8"></a>`description` | `"Backblaze B2-backed Riverhog archive and retrieval store."` |
| <a id="s-a3cbcfe217"></a>`distribution_roots` | `["a-riverhog-b2-store"]` |
| <a id="s-712e681f1f"></a>`format` | `"oci-image"` |
| <a id="s-0dd11b8bfd"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-8f5a6763eb"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-67bb10214a"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-20fcf0af7c"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/a-riverhog-b2-store","kind":"oci-repository"}` |
| <a id="s-b50075eb12"></a>`repository` | `"ghcr.io/nashspence/a-riverhog-b2-store"` |
| <a id="s-c4b90e6c69"></a>`role` | `"component"` |
| <a id="s-d153da3aea"></a>`tag_templates` | `["ghcr.io/nashspence/a-riverhog-b2-store:{version}","ghcr.io/nashspence/a-riverhog-b2-store:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-b2-store](../../../evidence/relationships/nodes.md#rn-6727835d85)

## Governing policies

- <a id="pa-c2cbb3627b"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-d9265cecd4"></a>[publication/image-digest-scope/v1](../../../policies/publication-image-digest-scope-v1/index.md#p-634e69c23f)
- <a id="pa-29a3b830f2"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-95229c7aa8"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/a-riverhog-b2-store`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ea5db5d38481e8700a58f2a24f92e7a7705d2c3d795c3fa49ac2c965072bb72 -->

```json
{
  "build_target": "a-riverhog-b2-store",
  "description": "Backblaze B2-backed Riverhog archive and retrieval store.",
  "distribution_roots": [
    "a-riverhog-b2-store"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/a-riverhog-b2-store",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/a-riverhog-b2-store",
  "role": "component",
  "tag_templates": [
    "ghcr.io/nashspence/a-riverhog-b2-store:{version}",
    "ghcr.io/nashspence/a-riverhog-b2-store:sha-{source_sha}"
  ]
}
```

</details>
