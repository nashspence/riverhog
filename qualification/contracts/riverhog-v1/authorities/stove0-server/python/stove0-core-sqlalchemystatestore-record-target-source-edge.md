# stove0_core.SqlAlchemyStateStore.record_target_source_edge

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-record-t-e130fca9c1:bb95cdb9c7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3c95f6ac72"></a>
- <a id="s-40ce57ad0d"></a>`distribution`: `stove0-server`
- <a id="s-8a94ffb70b"></a>`module`: `stove0_core`
- <a id="s-86702812b3"></a>`name`: `record_target_source_edge`
- <a id="s-bf5e6c8c84"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-3e236cf171"></a>`unit`: `member`

### Declared structure

- <a id="s-a62e49a03e"></a>`kind`: `"method"`
- <a id="s-cb0931011f"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', edge: 'OutputSourceEdge') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-707762e687"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.record_target_source_edge`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5d46dd742dd7b37506e0c47ecb064a2bc27dcc0e987a63de98be3931088b7767 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', edge: 'OutputSourceEdge') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_target_source_edge",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
