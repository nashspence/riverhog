# Release artifact: documentation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release-artifacts:release:release-artifact-documentation:eb17c29147 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release Artifacts](index.md) |

## External contract

<a id="s-b88b99aa1c"></a>
| Concern | Contract |
|---|---|
| <a id="s-4721bb9084"></a>`coordinate` | riverhog-docs-v{version}.tar.gz |
| <a id="s-c79a7486a2"></a>`format` | tar+gzip |

## Governing policies

- <a id="pa-cc6111216d"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/release_artifacts/documentation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8fe97303a4fb16dc1828628310a554393f40262816503291041cc59d56b8351d -->

```json
{
  "coordinate": "riverhog-docs-v{version}.tar.gz",
  "format": "tar+gzip"
}
```

</details>
