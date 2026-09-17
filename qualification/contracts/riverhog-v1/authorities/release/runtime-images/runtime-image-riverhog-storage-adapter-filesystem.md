# Runtime image: riverhog-storage-adapter-filesystem

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-riverhog-storage-adapter-filesystem:30cc000638 -->

Optional nonnormative Linux filesystem storage reference for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-85a35d7977"></a>
| Concern | Contract |
|---|---|
| <a id="s-e2299de9b7"></a>`build_target` | `"riverhog-storage-adapter-filesystem"` |
| <a id="s-f1520cc27d"></a>`description` | `"Optional nonnormative Linux filesystem storage reference for Riverhog."` |
| <a id="s-f627f0712c"></a>`distribution_roots` | `["riverhog-storage-adapter-filesystem"]` |
| <a id="s-2f65603f9b"></a>`format` | `"oci-image"` |
| <a id="s-25f68edfcc"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-5e372eaaf0"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-9eddfa0176"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-c54937e253"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/riverhog-storage-adapter-filesystem","kind":"oci-repository"}` |
| <a id="s-150ca33870"></a>`repository` | `"ghcr.io/nashspence/riverhog-storage-adapter-filesystem"` |
| <a id="s-0c7b9069ce"></a>`role` | `"reference"` |
| <a id="s-6418eb9e41"></a>`tag_templates` | `["ghcr.io/nashspence/riverhog-storage-adapter-filesystem:{version}","ghcr.io/nashspence/riverhog-storage-adapter-filesystem:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-filesystem](../../../evidence/relationships/nodes.md#rn-ebe4206627)

## Governing policies

- <a id="pa-efafbebafa"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-6760c73b64"></a>[publication/image-digest-scope/v1](../../../policies/publication-image-digest-scope-v1/index.md#p-634e69c23f)
- <a id="pa-2cbc7c8ebb"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-1c281f5c48"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/riverhog-storage-adapter-filesystem`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 130dcf2a34a04a759a45a3ef2d631c5baef9e8d90547a1578c790eb56a34df78 -->

```json
{
  "build_target": "riverhog-storage-adapter-filesystem",
  "description": "Optional nonnormative Linux filesystem storage reference for Riverhog.",
  "distribution_roots": [
    "riverhog-storage-adapter-filesystem"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/riverhog-storage-adapter-filesystem",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/riverhog-storage-adapter-filesystem",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-storage-adapter-filesystem:{version}",
    "ghcr.io/nashspence/riverhog-storage-adapter-filesystem:sha-{source_sha}"
  ]
}
```

</details>
