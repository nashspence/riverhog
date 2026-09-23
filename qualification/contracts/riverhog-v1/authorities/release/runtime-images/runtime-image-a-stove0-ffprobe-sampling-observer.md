# Runtime image: a-stove0-ffprobe-sampling-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-a-stove0-ffprobe-sampling-observer:67a81ac5dd -->

FFprobe media sampling observer for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-dabd822ce7"></a>
| Concern | Contract |
|---|---|
| <a id="s-e6cbe5d158"></a>`build_target` | `"a-stove0-ffprobe-sampling-observer"` |
| <a id="s-7ad211a58d"></a>`description` | `"FFprobe media sampling observer for Stove0."` |
| <a id="s-eef96b2e8e"></a>`distribution_roots` | `["a-stove0-ffprobe-sampling-observer"]` |
| <a id="s-ba7ad3a629"></a>`format` | `"oci-image"` |
| <a id="s-409cdfff4f"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-ceef1f05da"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-8712fb477d"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-6d509364e3"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/a-stove0-ffprobe-sampling-observer","kind":"oci-repository"}` |
| <a id="s-cdc1f0133f"></a>`repository` | `"ghcr.io/nashspence/a-stove0-ffprobe-sampling-observer"` |
| <a id="s-dc4b95c098"></a>`role` | `"component"` |
| <a id="s-220ae62068"></a>`tag_templates` | `["ghcr.io/nashspence/a-stove0-ffprobe-sampling-observer:{version}","ghcr.io/nashspence/a-stove0-ffprobe-sampling-observer:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-ffprobe-sampling-observer](../../../evidence/relationships/nodes.md#rn-cacf6cd019)

## Governing policies

- <a id="pa-99b89e8ae7"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-916f3535fe"></a>[publication/image-identity-scope/v1](../../../policies/publication-image-identity-scope-v1/index.md#p-8fb44d2436)
- <a id="pa-ff0a9d206e"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-70965709ec"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/a-stove0-ffprobe-sampling-observer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a0f6bb7d0339fd60657076b206800d21408b0358a024baa986778a343b619ae -->

```json
{
  "build_target": "a-stove0-ffprobe-sampling-observer",
  "description": "FFprobe media sampling observer for Stove0.",
  "distribution_roots": [
    "a-stove0-ffprobe-sampling-observer"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/a-stove0-ffprobe-sampling-observer",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/a-stove0-ffprobe-sampling-observer",
  "role": "component",
  "tag_templates": [
    "ghcr.io/nashspence/a-stove0-ffprobe-sampling-observer:{version}",
    "ghcr.io/nashspence/a-stove0-ffprobe-sampling-observer:sha-{source_sha}"
  ]
}
```

</details>
