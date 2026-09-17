# Release artifact: evidence:SHA256SUMS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release-artifacts:release:release-artifact-evidence-sha256sums:dfef5c2a25 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release Artifacts](index.md) |

## External contract

<a id="s-3fb92856d3"></a>
| Concern | Contract |
|---|---|
| <a id="s-919dc332b4"></a>`coordinate` | `"SHA256SUMS"` |
| <a id="s-78883970ed"></a>`format` | `"sha256-checksum-list"` |

## Governing policies

- <a id="pa-25d5299c85"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

- `/external_contract/release/publication/release_artifacts/evidence:SHA256SUMS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 925336f735755517dbcc9f4c2cad1c05f320f06c02db67ed47f556ba14be10ba -->

```json
{
  "coordinate": "SHA256SUMS",
  "format": "sha256-checksum-list"
}
```

</details>
