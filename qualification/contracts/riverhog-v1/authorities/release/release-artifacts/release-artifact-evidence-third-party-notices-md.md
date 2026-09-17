# Release artifact: evidence:THIRD_PARTY_NOTICES.md

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release-artifacts:release:release-artifact-evidence-third-party-notices-md:f81129108d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release Artifacts](index.md) |

## External contract

<a id="s-d0998191f7"></a>
| Concern | Contract |
|---|---|
| <a id="s-26be60bd23"></a>`coordinate` | `"THIRD_PARTY_NOTICES.md"` |
| <a id="s-7be800d6a0"></a>`format` | `"markdown"` |

## Governing policies

- <a id="pa-701f0e2211"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/release_artifacts/evidence:THIRD_PARTY_NOTICES.md`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82a9327d047142077a40a30149203bafa861268bdc3a3ed542c0845d6d57a588 -->

```json
{
  "coordinate": "THIRD_PARTY_NOTICES.md",
  "format": "markdown"
}
```

</details>
