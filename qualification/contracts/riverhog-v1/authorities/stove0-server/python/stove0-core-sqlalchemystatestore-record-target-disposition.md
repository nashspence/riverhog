# stove0_core.SqlAlchemyStateStore.record_target_disposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-record-t-26b6067bf2:ce4deeff9b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c0d4e13fd"></a>
- <a id="s-eef4375a3d"></a>`distribution`: `stove0-server`
- <a id="s-dbfc61a4bd"></a>`module`: `stove0_core`
- <a id="s-211f40c1a9"></a>`name`: `record_target_disposition`
- <a id="s-f73674fb61"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-f6ff3a2963"></a>`unit`: `member`

### Declared structure

- <a id="s-b5f279e4e6"></a>`kind`: `"method"`
- <a id="s-9c0529746b"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-e02a45c6a5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.record_target_disposition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37e2012b4e4ad0cdfb20fc1d9a3a8315b4e4de295d0bcd547a8ada940444dbeb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_target_disposition",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
