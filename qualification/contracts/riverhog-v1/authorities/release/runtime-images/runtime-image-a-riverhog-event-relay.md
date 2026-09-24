# Runtime image: a-riverhog-event-relay

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-a-riverhog-event-relay:c0447a7481 -->

Native lifecycle-event to CloudEvents webhook relay.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-398a8656e3"></a>
| Concern | Contract |
|---|---|
| <a id="s-9b8c12749d"></a>`build_target` | `"a-riverhog-event-relay"` |
| <a id="s-3fc650c5ec"></a>`description` | `"Native lifecycle-event to CloudEvents webhook relay."` |
| <a id="s-40f7c52372"></a>`distribution_roots` | `["a-riverhog-event-relay"]` |
| <a id="s-9cc51d1922"></a>`format` | `"oci-image"` |
| <a id="s-aa4306ebb9"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-33d571f0dc"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-aa98a99d44"></a>`platforms` | `["linux/amd64"]` |
| <a id="s-fdb2985a5c"></a>`publication_identity` | `{"coordinate":"ghcr.io/nashspence/a-riverhog-event-relay","kind":"oci-repository"}` |
| <a id="s-a54a86fa83"></a>`repository` | `"ghcr.io/nashspence/a-riverhog-event-relay"` |
| <a id="s-d877e3664a"></a>`role` | `"application"` |
| <a id="s-29850a4029"></a>`tag_templates` | `["ghcr.io/nashspence/a-riverhog-event-relay:{version}","ghcr.io/nashspence/a-riverhog-event-relay:sha-{source_sha}"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-event-relay](../../../evidence/relationships/nodes.md#rn-ad16b219fd)

## Governing policies

- <a id="pa-97033aa434"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-0657df1b69"></a>[publication/image-identity-scope/v1](../../../policies/publication-image-identity-scope-v1/index.md#p-8fb44d2436)
- <a id="pa-1197fdcd25"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-e65dd00f7d"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/a-riverhog-event-relay`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d53e7ab0a127d0e4e001790e3ce87296bd236f9fffaa84493ab748b1496a327d -->

```json
{
  "build_target": "a-riverhog-event-relay",
  "description": "Native lifecycle-event to CloudEvents webhook relay.",
  "distribution_roots": [
    "a-riverhog-event-relay"
  ],
  "format": "oci-image",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "platforms": [
    "linux/amd64"
  ],
  "publication_identity": {
    "coordinate": "ghcr.io/nashspence/a-riverhog-event-relay",
    "kind": "oci-repository"
  },
  "repository": "ghcr.io/nashspence/a-riverhog-event-relay",
  "role": "application",
  "tag_templates": [
    "ghcr.io/nashspence/a-riverhog-event-relay:{version}",
    "ghcr.io/nashspence/a-riverhog-event-relay:sha-{source_sha}"
  ]
}
```

</details>
