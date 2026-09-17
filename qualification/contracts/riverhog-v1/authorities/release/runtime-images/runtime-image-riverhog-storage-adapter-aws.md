# Runtime image: riverhog-storage-adapter-aws

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-riverhog-storage-adapter-aws:20d614d665 -->

Optional nonnormative AWS storage reference for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-d90ca4ab11"></a>
| Concern | Contract |
|---|---|
| <a id="s-80cb157b52"></a>`build_target` | `"riverhog-storage-adapter-aws"` |
| <a id="s-2be3a6ebff"></a>`description` | `"Optional nonnormative AWS storage reference for Riverhog."` |
| <a id="s-471d6d0a25"></a>`distribution_roots` | `["riverhog-storage-adapter-aws"]` |
| <a id="s-37c9a8f589"></a>`format` | `"oci-image"` |
| <a id="s-1c54fc1207"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-2679404553"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-6c4435dc28"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-739600f34c"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/riverhog-storage-adapter-aws","kind":"oci-repository"}` |
| <a id="s-2d2af6f16d"></a>`repository` | `"ghcr.io/nashspence/riverhog-storage-adapter-aws"` |
| <a id="s-5558a4421f"></a>`role` | `"reference"` |
| <a id="s-3342698ea1"></a>`tag_templates` | `["ghcr.io/nashspence/riverhog-storage-adapter-aws:{version}","ghcr.io/nashspence/riverhog-storage-adapter-aws:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-aws](../../../evidence/relationships/nodes.md#rn-c53f4ff3ef)

## Governing policies

- <a id="pa-9b61c4ae47"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-c170db255b"></a>[publication/image-digest-scope/v1](../../../policies/publication-image-digest-scope-v1/index.md#p-634e69c23f)
- <a id="pa-d700a07a3a"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-2477d4c127"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/riverhog-storage-adapter-aws`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7028d517bc0fc2eeba32f947604dbe25162e6936a2482b4b87957f90e321a46f -->

```json
{
  "build_target": "riverhog-storage-adapter-aws",
  "description": "Optional nonnormative AWS storage reference for Riverhog.",
  "distribution_roots": [
    "riverhog-storage-adapter-aws"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/riverhog-storage-adapter-aws",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/riverhog-storage-adapter-aws",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-storage-adapter-aws:{version}",
    "ghcr.io/nashspence/riverhog-storage-adapter-aws:sha-{source_sha}"
  ]
}
```

</details>
