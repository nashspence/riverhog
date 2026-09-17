# Runtime image: stove0-ffprobe-sampling-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-stove0-ffprobe-sampling-observer:fe78c9ec84 -->

Optional nonnormative FFprobe sampling-observer reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-75779d31c6"></a>
| Concern | Contract |
|---|---|
| <a id="s-ef9f9661e5"></a>`build_target` | `"stove0-ffprobe-sampling-observer"` |
| <a id="s-986e2cc516"></a>`description` | `"Optional nonnormative FFprobe sampling-observer reference for Stove0."` |
| <a id="s-d8eba62227"></a>`distribution_roots` | `["stove0-ffprobe-sampling-observer"]` |
| <a id="s-568cdf41aa"></a>`format` | `"oci-image"` |
| <a id="s-1ae576c36b"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-708a727dc3"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-ef0ea0a137"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-ed11ca8b91"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/riverhog-stove0-ffprobe-sampling-observer","kind":"oci-repository"}` |
| <a id="s-a2225ac56b"></a>`repository` | `"ghcr.io/nashspence/riverhog-stove0-ffprobe-sampling-observer"` |
| <a id="s-29252224bb"></a>`role` | `"reference"` |
| <a id="s-4788e659fb"></a>`tag_templates` | `["ghcr.io/nashspence/riverhog-stove0-ffprobe-sampling-observer:{version}","ghcr.io/nashspence/riverhog-stove0-ffprobe-sampling-observer:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-ffprobe-sampling-observer](../../../evidence/relationships/nodes.md#rn-219fad9b72)

## Governing policies

- <a id="pa-2709e61df0"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-f838df0944"></a>[publication/image-digest-scope/v1](../../../policies/publication-image-digest-scope-v1/index.md#p-634e69c23f)
- <a id="pa-8f9aedeed8"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-97f3fa7dc0"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/stove0-ffprobe-sampling-observer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 995397f3ab7ab426ca7aede917844c694db1145a64109ca06c05f7a4c81675fc -->

```json
{
  "build_target": "stove0-ffprobe-sampling-observer",
  "description": "Optional nonnormative FFprobe sampling-observer reference for Stove0.",
  "distribution_roots": [
    "stove0-ffprobe-sampling-observer"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/riverhog-stove0-ffprobe-sampling-observer",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/riverhog-stove0-ffprobe-sampling-observer",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-stove0-ffprobe-sampling-observer:{version}",
    "ghcr.io/nashspence/riverhog-stove0-ffprobe-sampling-observer:sha-{source_sha}"
  ]
}
```

</details>
