# Runtime image: a-stove0-exiftool-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-a-stove0-exiftool-observer:aebd382412 -->

ExifTool media metadata observer for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-7e213dd3fb"></a>
| Concern | Contract |
|---|---|
| <a id="s-3f153ab07d"></a>`build_target` | `"a-stove0-exiftool-observer"` |
| <a id="s-7fb31b829b"></a>`description` | `"ExifTool media metadata observer for Stove0."` |
| <a id="s-bd8ec7fc2f"></a>`distribution_roots` | `["a-stove0-exiftool-observer"]` |
| <a id="s-e34597c04a"></a>`format` | `"oci-image"` |
| <a id="s-e707f105d0"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-5d00663c93"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-59eaa2db4d"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-7ea411c63c"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/a-stove0-exiftool-observer","kind":"oci-repository"}` |
| <a id="s-47a15c343b"></a>`repository` | `"ghcr.io/nashspence/a-stove0-exiftool-observer"` |
| <a id="s-b3d63f1cac"></a>`role` | `"component"` |
| <a id="s-cb686eafd5"></a>`tag_templates` | `["ghcr.io/nashspence/a-stove0-exiftool-observer:{version}","ghcr.io/nashspence/a-stove0-exiftool-observer:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-exiftool-observer](../../../evidence/relationships/nodes.md#rn-5dd092712c)

## Governing policies

- <a id="pa-be39fc0fa2"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-35ffe7e465"></a>[publication/image-digest-scope/v1](../../../policies/publication-image-digest-scope-v1/index.md#p-634e69c23f)
- <a id="pa-1919f15283"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-47d8bfe93d"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/a-stove0-exiftool-observer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 72cbed05131593e70890f2960d04ac470cf32e65c175e2f042a88619265af41f -->

```json
{
  "build_target": "a-stove0-exiftool-observer",
  "description": "ExifTool media metadata observer for Stove0.",
  "distribution_roots": [
    "a-stove0-exiftool-observer"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/a-stove0-exiftool-observer",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/a-stove0-exiftool-observer",
  "role": "component",
  "tag_templates": [
    "ghcr.io/nashspence/a-stove0-exiftool-observer:{version}",
    "ghcr.io/nashspence/a-stove0-exiftool-observer:sha-{source_sha}"
  ]
}
```

</details>
