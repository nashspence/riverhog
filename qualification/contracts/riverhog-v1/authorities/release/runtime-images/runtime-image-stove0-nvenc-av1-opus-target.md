# Runtime image: stove0-nvenc-av1-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-stove0-nvenc-av1-opus-target:f02d00e64b -->

Optional nonnormative NVENC AV1 and Opus target reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-ab958bb633"></a>
| Concern | Contract |
|---|---|
| <a id="s-57bf4a92e1"></a>`build_target` | `"stove0-nvenc-av1-opus-target"` |
| <a id="s-e6937f1ff9"></a>`description` | `"Optional nonnormative NVENC AV1 and Opus target reference for Stove0."` |
| <a id="s-68ae32ea1e"></a>`distribution_roots` | `["stove0-nvenc-av1-opus-target","stove0-nvenc-av1-opus-review-sampler"]` |
| <a id="s-e90eb68c3e"></a>`format` | `"oci-image"` |
| <a id="s-3d2de7c1d1"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-6aa0f5b854"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-5e869d6a92"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-ce75a8bc26"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/riverhog-stove0-nvenc-av1-opus-target","kind":"oci-repository"}` |
| <a id="s-0c727c2e80"></a>`repository` | `"ghcr.io/nashspence/riverhog-stove0-nvenc-av1-opus-target"` |
| <a id="s-ec04038413"></a>`role` | `"reference"` |
| <a id="s-11b5f77caa"></a>`tag_templates` | `["ghcr.io/nashspence/riverhog-stove0-nvenc-av1-opus-target:{version}","ghcr.io/nashspence/riverhog-stove0-nvenc-av1-opus-target:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-nvenc-av1-opus-target](../../../evidence/relationships.md#rn-e9196fec57)
- [stove0-nvenc-av1-opus-review-sampler](../../../evidence/relationships.md#rn-3d4e1c390a)

## Governing policies

- <a id="pa-8c8b931d1c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-d2c205f397"></a>[publication/image-digest-scope/v1](../../../policies/index.md#p-634e69c23f)
- <a id="pa-0f3fce59d5"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-df84b2d1a4"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-images:docker-bake](../../../evidence/sources.md#src-8d3f4df21c) — `docker-bake.hcl`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/runtime_images/stove0-nvenc-av1-opus-target`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 32644cf0ee508cc812e1d28de9d76b264e0df33e51e4c69e588782b9d944b904 -->

```json
{
  "build_target": "stove0-nvenc-av1-opus-target",
  "description": "Optional nonnormative NVENC AV1 and Opus target reference for Stove0.",
  "distribution_roots": [
    "stove0-nvenc-av1-opus-target",
    "stove0-nvenc-av1-opus-review-sampler"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/riverhog-stove0-nvenc-av1-opus-target",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/riverhog-stove0-nvenc-av1-opus-target",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-stove0-nvenc-av1-opus-target:{version}",
    "ghcr.io/nashspence/riverhog-stove0-nvenc-av1-opus-target:sha-{source_sha}"
  ]
}
```

</details>
