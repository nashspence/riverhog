# Release artifact: contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release-artifacts:release:release-artifact-contract:9877f4eee9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release Artifacts](index.md) |

## External contract

<a id="s-f19a7e33ec"></a>
| Concern | Contract |
|---|---|
| <a id="s-bd0b418748"></a>`coordinate` | riverhog-v1-contract.tar.gz |
| <a id="s-a6d9956e57"></a>`format` | tar+gzip |

## Governing policies

- <a id="pa-6364a6118b"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

- `/external_contract/release/publication/release_artifacts/contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5831227b9fa5690c96d399cf3c46aeb953f6445898cb71c1d6ced1b83d8966ee -->

```json
{
  "coordinate": "riverhog-v1-contract.tar.gz",
  "format": "tar+gzip"
}
```

</details>
