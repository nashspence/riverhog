# Runtime image: a-riverhog-aws-store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-a-riverhog-aws-store:75253db857 -->

AWS-backed Riverhog archive and retrieval store.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-6462dcc807"></a>
| Concern | Contract |
|---|---|
| <a id="s-1877d99419"></a>`build_target` | `"a-riverhog-aws-store"` |
| <a id="s-750c03763b"></a>`description` | `"AWS-backed Riverhog archive and retrieval store."` |
| <a id="s-63f88586bc"></a>`distribution_roots` | `["a-riverhog-aws-store"]` |
| <a id="s-b75f6838a9"></a>`format` | `"oci-image"` |
| <a id="s-b8eab9595a"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-5943c44bcd"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-74d4a3e4d0"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-aa36f8b970"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/a-riverhog-aws-store","kind":"oci-repository"}` |
| <a id="s-ba4cb5a23e"></a>`repository` | `"ghcr.io/nashspence/a-riverhog-aws-store"` |
| <a id="s-02ccd53a8f"></a>`role` | `"component"` |
| <a id="s-a028d5324f"></a>`tag_templates` | `["ghcr.io/nashspence/a-riverhog-aws-store:{version}","ghcr.io/nashspence/a-riverhog-aws-store:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-aws-store](../../../evidence/relationships/nodes.md#rn-b19c22f180)

## Governing policies

- <a id="pa-69c459b71a"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-40f2bfb96d"></a>[publication/image-digest-scope/v1](../../../policies/publication-image-digest-scope-v1/index.md#p-634e69c23f)
- <a id="pa-8bbf703bc1"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-9b973bf0a8"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/a-riverhog-aws-store`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2aae934b30f8c35cba05b0449003331785f3209fb5dc7acf14596faf2b6e71ad -->

```json
{
  "build_target": "a-riverhog-aws-store",
  "description": "AWS-backed Riverhog archive and retrieval store.",
  "distribution_roots": [
    "a-riverhog-aws-store"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/a-riverhog-aws-store",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/a-riverhog-aws-store",
  "role": "component",
  "tag_templates": [
    "ghcr.io/nashspence/a-riverhog-aws-store:{version}",
    "ghcr.io/nashspence/a-riverhog-aws-store:sha-{source_sha}"
  ]
}
```

</details>
