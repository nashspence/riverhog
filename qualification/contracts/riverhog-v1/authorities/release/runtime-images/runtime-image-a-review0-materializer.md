# Runtime image: a-review0-materializer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-a-review0-materializer:9324f53607 -->

Review0 materialization target for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-eef23ba2ed"></a>
| Concern | Contract |
|---|---|
| <a id="s-af52f91e3d"></a>`build_target` | `"a-review0-materializer"` |
| <a id="s-fa456b6c88"></a>`description` | `"Review0 materialization target for Stove0."` |
| <a id="s-6c25daf6af"></a>`distribution_roots` | `["a-review0-materializer"]` |
| <a id="s-e76b751ce7"></a>`format` | `"oci-image"` |
| <a id="s-41b1336214"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-9e946e003b"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-f467c44248"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-fc0a24ed98"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/a-review0-materializer","kind":"oci-repository"}` |
| <a id="s-d918f71dc5"></a>`repository` | `"ghcr.io/nashspence/a-review0-materializer"` |
| <a id="s-e59758299f"></a>`role` | `"component"` |
| <a id="s-6fc3ab7467"></a>`tag_templates` | `["ghcr.io/nashspence/a-review0-materializer:{version}","ghcr.io/nashspence/a-review0-materializer:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-review0-materializer](../../../evidence/relationships/nodes.md#rn-df33691267)

## Governing policies

- <a id="pa-b26c7e0f46"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-8b2d46e2fe"></a>[publication/image-digest-scope/v1](../../../policies/publication-image-digest-scope-v1/index.md#p-634e69c23f)
- <a id="pa-04d0317475"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-e0542957b1"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/a-review0-materializer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 695d514e5bcaedd2398e064d7c19fb424ad8999c38ceed1ffb2ef6218ebbadc5 -->

```json
{
  "build_target": "a-review0-materializer",
  "description": "Review0 materialization target for Stove0.",
  "distribution_roots": [
    "a-review0-materializer"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/a-review0-materializer",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/a-review0-materializer",
  "role": "component",
  "tag_templates": [
    "ghcr.io/nashspence/a-review0-materializer:{version}",
    "ghcr.io/nashspence/a-review0-materializer:sha-{source_sha}"
  ]
}
```

</details>
