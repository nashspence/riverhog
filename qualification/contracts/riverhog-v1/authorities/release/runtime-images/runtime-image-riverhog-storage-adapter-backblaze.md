# Runtime image: riverhog-storage-adapter-backblaze

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-riverhog-storage-adapter-backblaze:a9ce012b4f -->

Optional nonnormative Backblaze B2 storage reference for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-8ed8987c04"></a>
| Concern | Contract |
|---|---|
| <a id="s-8de919ee8b"></a>`build_target` | `"riverhog-storage-adapter-backblaze"` |
| <a id="s-d7d3447989"></a>`description` | `"Optional nonnormative Backblaze B2 storage reference for Riverhog."` |
| <a id="s-2760bc934b"></a>`distribution_roots` | `["riverhog-storage-adapter-backblaze"]` |
| <a id="s-2e2dba66bd"></a>`format` | `"oci-image"` |
| <a id="s-6665e8a59c"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-5b76337e31"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-d84f7a1e41"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-340dbd6cc9"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/riverhog-storage-adapter-backblaze","kind":"oci-repository"}` |
| <a id="s-cf5084e717"></a>`repository` | `"ghcr.io/nashspence/riverhog-storage-adapter-backblaze"` |
| <a id="s-f37fe9c223"></a>`role` | `"reference"` |
| <a id="s-97ee88894f"></a>`tag_templates` | `["ghcr.io/nashspence/riverhog-storage-adapter-backblaze:{version}","ghcr.io/nashspence/riverhog-storage-adapter-backblaze:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-backblaze](../../../evidence/relationships.md#rn-4178e6e967)

## Governing policies

- <a id="pa-b8df4418d1"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-8b5d5467a8"></a>[publication/image-digest-scope/v1](../../../policies/index.md#p-634e69c23f)
- <a id="pa-8e05911915"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-cc78ec7247"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-images:docker-bake](../../../evidence/sources.md#src-8d3f4df21c) — [docker-bake.hcl](../../../../../../docker-bake.hcl)
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/runtime_images/riverhog-storage-adapter-backblaze`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 723a3b646a523cff970915c24dd7b1defcc92e3b05efcd290eea0efa983f1a66 -->

```json
{
  "build_target": "riverhog-storage-adapter-backblaze",
  "description": "Optional nonnormative Backblaze B2 storage reference for Riverhog.",
  "distribution_roots": [
    "riverhog-storage-adapter-backblaze"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/riverhog-storage-adapter-backblaze",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/riverhog-storage-adapter-backblaze",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-storage-adapter-backblaze:{version}",
    "ghcr.io/nashspence/riverhog-storage-adapter-backblaze:sha-{source_sha}"
  ]
}
```

</details>
