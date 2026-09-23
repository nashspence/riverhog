# Runtime image: a-riverhog-ftp-spool

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-a-riverhog-ftp-spool:29e17707f2 -->

FTP upload spool and ingestion adapter for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-3afd341810"></a>
| Concern | Contract |
|---|---|
| <a id="s-c58997a9a7"></a>`build_target` | `"a-riverhog-ftp-spool"` |
| <a id="s-5406367a08"></a>`description` | `"FTP upload spool and ingestion adapter for Riverhog."` |
| <a id="s-5ec08a3702"></a>`distribution_roots` | `["a-riverhog-ftp-spool","a-riverhog-linux-provenance-observer"]` |
| <a id="s-2b48b75b66"></a>`format` | `"oci-image"` |
| <a id="s-6f681e6432"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-3311359e30"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-0b2aae4b51"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-c64389ef9f"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/a-riverhog-ftp-spool","kind":"oci-repository"}` |
| <a id="s-84a0523256"></a>`repository` | `"ghcr.io/nashspence/a-riverhog-ftp-spool"` |
| <a id="s-dc775deb65"></a>`role` | `"component"` |
| <a id="s-422c614c85"></a>`tag_templates` | `["ghcr.io/nashspence/a-riverhog-ftp-spool:{version}","ghcr.io/nashspence/a-riverhog-ftp-spool:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-ftp-spool](../../../evidence/relationships/nodes.md#rn-507e6fb957)
- [a-riverhog-linux-provenance-observer](../../../evidence/relationships/nodes.md#rn-84baf121e9)

## Governing policies

- <a id="pa-805ddab07a"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-d4c1b38dd2"></a>[publication/image-digest-scope/v1](../../../policies/publication-image-digest-scope-v1/index.md#p-634e69c23f)
- <a id="pa-20cbe5115f"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-4d0a357d4c"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/a-riverhog-ftp-spool`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1229c5f0c634f8c2c89604769844f703d791acfdd8f1c86b3a0446cda9fd056 -->

```json
{
  "build_target": "a-riverhog-ftp-spool",
  "description": "FTP upload spool and ingestion adapter for Riverhog.",
  "distribution_roots": [
    "a-riverhog-ftp-spool",
    "a-riverhog-linux-provenance-observer"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/a-riverhog-ftp-spool",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/a-riverhog-ftp-spool",
  "role": "component",
  "tag_templates": [
    "ghcr.io/nashspence/a-riverhog-ftp-spool:{version}",
    "ghcr.io/nashspence/a-riverhog-ftp-spool:sha-{source_sha}"
  ]
}
```

</details>
