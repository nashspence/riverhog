# stove0_core.Stove0WorkService.admit_branch_set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-admit-branch-set:12f9786969 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7416f54980"></a>
- <a id="s-76fba5086c"></a>`distribution`: `stove0-server`
- <a id="s-2e02275732"></a>`module`: `stove0_core`
- <a id="s-961cce3beb"></a>`name`: `admit_branch_set`
- <a id="s-53c125a20c"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-81a3ba95f4"></a>`unit`: `member`

### Declared structure

- <a id="s-8986c8be4a"></a>`kind`: `"method"`
- <a id="s-1bf18db6e1"></a>`signature`: `"\"(self, work_id: 'str', decision: 'BranchSetDecision', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-96d518c0f3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.admit_branch_set`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8ba5bbb23df1674ac2ee70c828b67bf0a5c15ad12d76d2066cf3b1db7b36d723 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', decision: 'BranchSetDecision', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "admit_branch_set",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```
