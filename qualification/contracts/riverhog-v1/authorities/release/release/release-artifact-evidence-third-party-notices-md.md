# Release artifact: evidence:THIRD_PARTY_NOTICES.md

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-artifact-evidence-third-party-notices-md:fee9f9631e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-d0998191f7"></a>
| Concern | Contract |
|---|---|
| <a id="s-26be60bd23"></a>`coordinate` | THIRD_PARTY_NOTICES.md |
| <a id="s-7be800d6a0"></a>`format` | markdown |

## Governing policies

- <a id="pa-5f77290708"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

- `/external_contract/release/publication/release_artifacts/evidence:THIRD_PARTY_NOTICES.md`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82a9327d047142077a40a30149203bafa861268bdc3a3ed542c0845d6d57a588 -->

```json
{
  "coordinate": "THIRD_PARTY_NOTICES.md",
  "format": "markdown"
}
```
