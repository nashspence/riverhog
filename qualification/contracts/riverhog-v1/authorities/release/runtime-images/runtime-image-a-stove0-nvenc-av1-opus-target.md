# Runtime image: a-stove0-nvenc-av1-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-a-stove0-nvenc-av1-opus-target:bf31307340 -->

NVENC AV1 and Opus transformation target for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-fe67fdb495"></a>
| Concern | Contract |
|---|---|
| <a id="s-58de3de26b"></a>`build_target` | `"a-stove0-nvenc-av1-opus-target"` |
| <a id="s-38cb914270"></a>`description` | `"NVENC AV1 and Opus transformation target for Stove0."` |
| <a id="s-dea30d2757"></a>`distribution_roots` | `["a-stove0-nvenc-av1-opus-target","a-review0-nvenc-av1-opus-sampler"]` |
| <a id="s-5a7d6fe50e"></a>`format` | `"oci-image"` |
| <a id="s-b897deba99"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-34f812eda3"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-736493a241"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-fbcd908932"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/a-stove0-nvenc-av1-opus-target","kind":"oci-repository"}` |
| <a id="s-6d5baf800c"></a>`repository` | `"ghcr.io/nashspence/a-stove0-nvenc-av1-opus-target"` |
| <a id="s-0421d78451"></a>`role` | `"component"` |
| <a id="s-af6d229776"></a>`tag_templates` | `["ghcr.io/nashspence/a-stove0-nvenc-av1-opus-target:{version}","ghcr.io/nashspence/a-stove0-nvenc-av1-opus-target:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-nvenc-av1-opus-target](../../../evidence/relationships/nodes.md#rn-7fd150cf21)
- [a-review0-nvenc-av1-opus-sampler](../../../evidence/relationships/nodes.md#rn-8138f74fd5)

## Governing policies

- <a id="pa-3b7f0a14f8"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-0b4a49c605"></a>[publication/image-identity-scope/v1](../../../policies/publication-image-identity-scope-v1/index.md#p-8fb44d2436)
- <a id="pa-c17e15a48d"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-70dda338a3"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/a-stove0-nvenc-av1-opus-target`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c73d8d9db22c9a0b1830a827ed0809c1b9fd49ebae5f11a893e06d5fcfd9cdce -->

```json
{
  "build_target": "a-stove0-nvenc-av1-opus-target",
  "description": "NVENC AV1 and Opus transformation target for Stove0.",
  "distribution_roots": [
    "a-stove0-nvenc-av1-opus-target",
    "a-review0-nvenc-av1-opus-sampler"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/a-stove0-nvenc-av1-opus-target",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/a-stove0-nvenc-av1-opus-target",
  "role": "component",
  "tag_templates": [
    "ghcr.io/nashspence/a-stove0-nvenc-av1-opus-target:{version}",
    "ghcr.io/nashspence/a-stove0-nvenc-av1-opus-target:sha-{source_sha}"
  ]
}
```

</details>
