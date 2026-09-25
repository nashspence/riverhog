# Runtime image: a-riverhog-opentimestamps-witness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-a-riverhog-opentimestamps-witness:0dddc09fbb -->

Independent OpenTimestamps collection witness for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-f9a8a757ee"></a>
| Concern | Contract |
|---|---|
| <a id="s-2860dac62f"></a>`build_target` | `"a-riverhog-opentimestamps-witness"` |
| <a id="s-f279aba9f8"></a>`description` | `"Independent OpenTimestamps collection witness for Riverhog."` |
| <a id="s-709fde281a"></a>`distribution_roots` | `["a-riverhog-opentimestamps-witness"]` |
| <a id="s-f85138633f"></a>`format` | `"oci-image"` |
| <a id="s-0e130a3f79"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-d704cc28a9"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-7aa836f83c"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-7f136e4369"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/a-riverhog-opentimestamps-witness","kind":"oci-repository"}` |
| <a id="s-98b2ea4d9e"></a>`repository` | `"ghcr.io/nashspence/a-riverhog-opentimestamps-witness"` |
| <a id="s-665ec7b37f"></a>`role` | `"application"` |
| <a id="s-be7a39db83"></a>`tag_templates` | `["ghcr.io/nashspence/a-riverhog-opentimestamps-witness:{version}","ghcr.io/nashspence/a-riverhog-opentimestamps-witness:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-opentimestamps-witness](../../../evidence/relationships/nodes.md#rn-44dc2bd3f2)

## Governing policies

- <a id="pa-b26384ae32"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-24045aac8f"></a>[publication/image-identity-scope/v1](../../../policies/publication-image-identity-scope-v1/index.md#p-8fb44d2436)
- <a id="pa-e576406504"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-94d5e7d0d3"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/a-riverhog-opentimestamps-witness`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eb5d9cb2348387261ede36e143983ec77e6240da70cd5d1e8373fdf08d4d09a6 -->

```json
{
  "build_target": "a-riverhog-opentimestamps-witness",
  "description": "Independent OpenTimestamps collection witness for Riverhog.",
  "distribution_roots": [
    "a-riverhog-opentimestamps-witness"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/a-riverhog-opentimestamps-witness",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/a-riverhog-opentimestamps-witness",
  "role": "application",
  "tag_templates": [
    "ghcr.io/nashspence/a-riverhog-opentimestamps-witness:{version}",
    "ghcr.io/nashspence/a-riverhog-opentimestamps-witness:sha-{source_sha}"
  ]
}
```

</details>
