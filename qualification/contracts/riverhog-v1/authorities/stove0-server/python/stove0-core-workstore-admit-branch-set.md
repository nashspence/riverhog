# stove0_core.WorkStore.admit_branch_set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-admit-branch-set:f5ae714f85 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cb874c0f02"></a>
| Field | Shape |
|---|---|
| <a id="s-91d44c6d40"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-22a89b249d"></a>`distribution` | "stove0-server" |
| <a id="s-b78fde379a"></a>`module` | "stove0_core" |
| <a id="s-26f0cea3e5"></a>`name` | "admit_branch_set" |
| <a id="s-f0e8d7fdd9"></a>`owner` | "stove0_core.WorkStore" |
| <a id="s-9558bd9335"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-0376b6ed93"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.admit_branch_set`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e520137d84e31a56a420d73c879fa0756f1a49dcfa50bbbd41808be8930b770 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', decision: 'BranchSetDecision') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "admit_branch_set",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
