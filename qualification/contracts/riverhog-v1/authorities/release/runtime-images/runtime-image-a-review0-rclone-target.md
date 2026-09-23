# Runtime image: a-review0-rclone-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-a-review0-rclone-target:65f7926d85 -->

Review0 rclone delivery target for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-46c8aaae92"></a>
| Concern | Contract |
|---|---|
| <a id="s-c142a753a0"></a>`build_target` | `"a-review0-rclone-target"` |
| <a id="s-911902850f"></a>`description` | `"Review0 rclone delivery target for Stove0."` |
| <a id="s-0481dd3682"></a>`distribution_roots` | `["a-review0-rclone-target"]` |
| <a id="s-ff0f054022"></a>`format` | `"oci-image"` |
| <a id="s-910ffac4d5"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-256d1a55a8"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-ffe05fef01"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-d463172d80"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/a-review0-rclone-target","kind":"oci-repository"}` |
| <a id="s-25e76764ab"></a>`repository` | `"ghcr.io/nashspence/a-review0-rclone-target"` |
| <a id="s-73252f418c"></a>`role` | `"component"` |
| <a id="s-33fdf3b6cd"></a>`tag_templates` | `["ghcr.io/nashspence/a-review0-rclone-target:{version}","ghcr.io/nashspence/a-review0-rclone-target:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-review0-rclone-target](../../../evidence/relationships/nodes.md#rn-b5b7de6707)

## Governing policies

- <a id="pa-8586c35088"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-b5ee8f202c"></a>[publication/image-identity-scope/v1](../../../policies/publication-image-identity-scope-v1/index.md#p-8fb44d2436)
- <a id="pa-0a14de8b01"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-085d8a18ac"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/a-review0-rclone-target`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f8990a405ba240bf1f0e5d3d3680270670aa1670e2e8f0bd96084b806620de9e -->

```json
{
  "build_target": "a-review0-rclone-target",
  "description": "Review0 rclone delivery target for Stove0.",
  "distribution_roots": [
    "a-review0-rclone-target"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/a-review0-rclone-target",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/a-review0-rclone-target",
  "role": "component",
  "tag_templates": [
    "ghcr.io/nashspence/a-review0-rclone-target:{version}",
    "ghcr.io/nashspence/a-review0-rclone-target:sha-{source_sha}"
  ]
}
```

</details>
