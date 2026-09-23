# Runtime image: a-stove0-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-a-stove0-opus-target:7fe7e205a6 -->

Opus transformation target for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-7aec98acce"></a>
| Concern | Contract |
|---|---|
| <a id="s-eb4ccc9593"></a>`build_target` | `"a-stove0-opus-target"` |
| <a id="s-b5e34d2a28"></a>`description` | `"Opus transformation target for Stove0."` |
| <a id="s-071bff6c81"></a>`distribution_roots` | `["a-stove0-opus-target","a-review0-opus-sampler"]` |
| <a id="s-bae13f07ee"></a>`format` | `"oci-image"` |
| <a id="s-323cf54b62"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-5a2ad2d674"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-b98f79c80f"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-2fe3983ea9"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/a-stove0-opus-target","kind":"oci-repository"}` |
| <a id="s-6e60df7553"></a>`repository` | `"ghcr.io/nashspence/a-stove0-opus-target"` |
| <a id="s-63747ca035"></a>`role` | `"component"` |
| <a id="s-15e6aaa4a0"></a>`tag_templates` | `["ghcr.io/nashspence/a-stove0-opus-target:{version}","ghcr.io/nashspence/a-stove0-opus-target:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-opus-target](../../../evidence/relationships/nodes.md#rn-9ac2ba7d16)
- [a-review0-opus-sampler](../../../evidence/relationships/nodes.md#rn-0d138eb003)

## Governing policies

- <a id="pa-364bc45505"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-83aa48c81d"></a>[publication/image-digest-scope/v1](../../../policies/publication-image-digest-scope-v1/index.md#p-634e69c23f)
- <a id="pa-465dd5e47f"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-b58d714d42"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/a-stove0-opus-target`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e299de70a25f6ced2a8b47074064ad7ee3578aa925253e3012f1a0b634d3b75b -->

```json
{
  "build_target": "a-stove0-opus-target",
  "description": "Opus transformation target for Stove0.",
  "distribution_roots": [
    "a-stove0-opus-target",
    "a-review0-opus-sampler"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/a-stove0-opus-target",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/a-stove0-opus-target",
  "role": "component",
  "tag_templates": [
    "ghcr.io/nashspence/a-stove0-opus-target:{version}",
    "ghcr.io/nashspence/a-stove0-opus-target:sha-{source_sha}"
  ]
}
```

</details>
