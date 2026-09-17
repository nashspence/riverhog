# Release artifact: evidence:release-manifest.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release-artifacts:release:release-artifact-evidence-release-manifest-json:18ba4db510 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release Artifacts](index.md) |

## External contract

<a id="s-6829dfc84b"></a>
| Concern | Contract |
|---|---|
| <a id="s-8271827bc7"></a>`coordinate` | `"release-manifest.json"` |
| <a id="s-feab459d84"></a>`format` | `"json"` |

## Governing policies

- <a id="pa-c2ffa53abc"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/release_artifacts/evidence:release-manifest.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f406c7f6e130a21c9bb3bf4edb0cd58bab22f675f5f1da30523964ad4a2056a1 -->

```json
{
  "coordinate": "release-manifest.json",
  "format": "json"
}
```

</details>
