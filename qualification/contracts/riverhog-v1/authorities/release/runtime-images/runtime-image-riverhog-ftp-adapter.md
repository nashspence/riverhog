# Runtime image: riverhog-ftp-adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: runtime-images:release:runtime-image-riverhog-ftp-adapter:51ca1541e0 -->

Optional nonnormative Riverhog FTP ingress reference.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Runtime Images](index.md) |

## External contract

<a id="s-c61679b7cd"></a>
| Concern | Contract |
|---|---|
| <a id="s-7433ad3c19"></a>`build_target` | riverhog-ftp-adapter |
| <a id="s-b5e0b20481"></a>`description` | Optional nonnormative Riverhog FTP ingress reference. |
| <a id="s-2905fb3f67"></a>`distribution_roots` | ["riverhog-ftp-adapter", "riverhog-provenance-linux-observer"] |
| <a id="s-1c6d8f7dd2"></a>`format` | oci-image |
| <a id="s-7a70bd1ec1"></a>`platforms` | ["linux/amd64"] |
| <a id="s-205d0879b6"></a>`repository` | ghcr.io/nashspence/riverhog-ftp-adapter |
| <a id="s-f309b94d3f"></a>`role` | reference |
| <a id="s-2741f48ff4"></a>`tag_templates` | ["ghcr.io/nashspence/riverhog-ftp-adapter:{version}", "ghcr.io/nashspence/riverhog-ftp-adapter:sha-{source_sha}"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-ftp-adapter](../../../evidence/relationships.md#rn-d738980294)
- [riverhog-provenance-linux-observer](../../../evidence/relationships.md#rn-f733d7aee6)

## Governing policies

- <a id="pa-ac1b0e9909"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ba21d1616c"></a>[publication/image-digest-scope/v1](../../../policies/index.md#p-634e69c23f)
- <a id="pa-4c9748b648"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-f901173fd8"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

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

- `/external_contract/release/publication/runtime_images/riverhog-ftp-adapter`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb8eb206dab9696bdb758b7638e52996847f110739e30ca53f0f75c0aa822974 -->

```json
{
  "build_target": "riverhog-ftp-adapter",
  "description": "Optional nonnormative Riverhog FTP ingress reference.",
  "distribution_roots": [
    "riverhog-ftp-adapter",
    "riverhog-provenance-linux-observer"
  ],
  "format": "oci-image",
  "platforms": [
    "linux/amd64"
  ],
  "repository": "ghcr.io/nashspence/riverhog-ftp-adapter",
  "role": "reference",
  "tag_templates": [
    "ghcr.io/nashspence/riverhog-ftp-adapter:{version}",
    "ghcr.io/nashspence/riverhog-ftp-adapter:sha-{source_sha}"
  ]
}
```
