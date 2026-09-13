# Release artifact: evidence:release.intoto.jsonl

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release-artifacts:release:release-artifact-evidence-release-intoto-jsonl:58bc70cad5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release Artifacts](index.md) |

## External contract

<a id="s-ce80dd3a60"></a>
| Concern | Contract |
|---|---|
| <a id="s-ed7dce09b3"></a>`coordinate` | release.intoto.jsonl |
| <a id="s-100e2d799e"></a>`format` | in-toto-jsonl |

## Governing policies

- <a id="pa-dd77faa107"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

- `/external_contract/release/publication/release_artifacts/evidence:release.intoto.jsonl`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f820195af1b634fa36975cfe7532810e2bbfaaca179f4dc8c275bccd9fa03267 -->

```json
{
  "coordinate": "release.intoto.jsonl",
  "format": "in-toto-jsonl"
}
```
