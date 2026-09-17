# Runtime image: stove0-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-stove0-opus-target:3a05a8d0f4 -->

Optional nonnormative Opus target reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-cb18d0661e"></a>
| Concern | Contract |
|---|---|
| <a id="s-c0a4ab77dc"></a>`build_target` | `"stove0-opus-target"` |
| <a id="s-d92b0b7680"></a>`description` | `"Optional nonnormative Opus target reference for Stove0."` |
| <a id="s-6f67b15332"></a>`distribution_roots` | `["stove0-opus-target","stove0-opus-review-sampler"]` |
| <a id="s-627b5e2cdc"></a>`format` | `"oci-image"` |
| <a id="s-5313035ce0"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-682eda2978"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-ea76c70402"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-63616d3996"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/riverhog-stove0-opus-target","kind":"oci-repository"}` |
| <a id="s-65067a059b"></a>`repository` | `"ghcr.io/nashspence/riverhog-stove0-opus-target"` |
| <a id="s-6b9062690b"></a>`role` | `"reference"` |
| <a id="s-20860bcd0d"></a>`tag_templates` | `["ghcr.io/nashspence/riverhog-stove0-opus-target:{version}","ghcr.io/nashspence/riverhog-stove0-opus-target:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-opus-target](../../../evidence/relationships/nodes.md#rn-313a5c450f)
- [stove0-opus-review-sampler](../../../evidence/relationships/nodes.md#rn-36756ccd77)

## Governing policies

- <a id="pa-7484c74640"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-851ed1a119"></a>[publication/image-digest-scope/v1](../../../policies/publication-image-digest-scope-v1/index.md#p-634e69c23f)
- <a id="pa-c2d97470b8"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-0bf3ec2364"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/stove0-opus-target`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a82ecccf8bca519d07442bdc83c18e3b120581bf972c95d380cdcbc5fc7effd -->

```json
{
  "build_target": "stove0-opus-target",
  "description": "Optional nonnormative Opus target reference for Stove0.",
  "distribution_roots": [
    "stove0-opus-target",
    "stove0-opus-review-sampler"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/riverhog-stove0-opus-target",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/riverhog-stove0-opus-target",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-stove0-opus-target:{version}",
    "ghcr.io/nashspence/riverhog-stove0-opus-target:sha-{source_sha}"
  ]
}
```

</details>
