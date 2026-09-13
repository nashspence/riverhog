# stove0_protocol.BranchSettlement.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsettlement-seal:cda774e315 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6ab17c4b36"></a>
| Field | Shape |
|---|---|
| <a id="s-cd712f3992"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-41dc9dd002"></a>`distribution` | "stove0-protocol" |
| <a id="s-4fcbffa40d"></a>`module` | "stove0_protocol" |
| <a id="s-2343f003e0"></a>`name` | "seal" |
| <a id="s-4a7de9cb31"></a>`owner` | "stove0_protocol.BranchSettlement" |
| <a id="s-21e1fcdec5"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.BranchSettlement](stove0-protocol-branchsettlement.md)

## Governing policies

- <a id="pa-ea6a5b6696"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSettlement.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b2366628400a9f05ad4060d4d83e83f9d2e6ad26d313b8255a3c1e75de5105b -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, branch: 'BranchPlan', derivation_sha256: 'str', producer_settlement_sha256: 'str', output_collection: 'CollectionRootRef', output_selection: 'ArtifactSelection') -> 'BranchSettlement'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.BranchSettlement",
  "unit": "member"
}
```
