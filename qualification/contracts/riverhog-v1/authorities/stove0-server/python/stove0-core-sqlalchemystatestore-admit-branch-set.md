# stove0_core.SqlAlchemyStateStore.admit_branch_set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-admit-branch-set:5612d4d663 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-93f2b5f893"></a>
- <a id="s-a170a37deb"></a>`distribution`: `stove0-server`
- <a id="s-e0b3e37011"></a>`module`: `stove0_core`
- <a id="s-d1c0260579"></a>`name`: `admit_branch_set`
- <a id="s-1c8333a45b"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-8bad08c3a6"></a>`unit`: `member`

### Declared structure

- <a id="s-db0dfb68b0"></a>`kind`: `"method"`
- <a id="s-f82859c57d"></a>`signature`: `"\"(self, work_id: 'str', *, expected_revision: 'int', decision: 'BranchSetDecision') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-940cbf3c42"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.admit_branch_set`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a6a9a0771201584e8909f6048900be54a5f3d43ef70d8aad0c0897bb20c604e5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', decision: 'BranchSetDecision') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "admit_branch_set",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
