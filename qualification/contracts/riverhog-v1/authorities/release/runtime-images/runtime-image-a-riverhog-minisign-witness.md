# Runtime image: a-riverhog-minisign-witness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-a-riverhog-minisign-witness:f0f32d04aa -->

Independent Minisign collection witness for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-b6968d40bb"></a>
| Concern | Contract |
|---|---|
| <a id="s-44578e0ac4"></a>`build_target` | `"a-riverhog-minisign-witness"` |
| <a id="s-3aeec4f6e3"></a>`description` | `"Independent Minisign collection witness for Riverhog."` |
| <a id="s-559ac4db31"></a>`distribution_roots` | `["a-riverhog-minisign-witness"]` |
| <a id="s-e367105959"></a>`format` | `"oci-image"` |
| <a id="s-d943aa7fb5"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-0a941d1739"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-12a98893e9"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-625ca5cd15"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/a-riverhog-minisign-witness","kind":"oci-repository"}` |
| <a id="s-7d7a135cbf"></a>`repository` | `"ghcr.io/nashspence/a-riverhog-minisign-witness"` |
| <a id="s-ca58cca2d5"></a>`role` | `"application"` |
| <a id="s-fd1329735e"></a>`tag_templates` | `["ghcr.io/nashspence/a-riverhog-minisign-witness:{version}","ghcr.io/nashspence/a-riverhog-minisign-witness:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-minisign-witness](../../../evidence/relationships/nodes.md#rn-169780f44a)

## Governing policies

- <a id="pa-b7b1a80d3a"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-b163f5d44b"></a>[publication/image-identity-scope/v1](../../../policies/publication-image-identity-scope-v1/index.md#p-8fb44d2436)
- <a id="pa-794c71969a"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-50af20b3af"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/a-riverhog-minisign-witness`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f3ea6ce1792e178fb39a324fce203ecba2f314fef2f718b320c77519d914e018 -->

```json
{
  "build_target": "a-riverhog-minisign-witness",
  "description": "Independent Minisign collection witness for Riverhog.",
  "distribution_roots": [
    "a-riverhog-minisign-witness"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/a-riverhog-minisign-witness",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/a-riverhog-minisign-witness",
  "role": "application",
  "tag_templates": [
    "ghcr.io/nashspence/a-riverhog-minisign-witness:{version}",
    "ghcr.io/nashspence/a-riverhog-minisign-witness:sha-{source_sha}"
  ]
}
```

</details>
