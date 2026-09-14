# stove0_core.SqlAlchemyStateStore.record_target_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-record-t-ca769afc14:0a4ba28c24 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4f3b1e8fa8"></a>
- <a id="s-3c65b10640"></a>`distribution`: `stove0-server`
- <a id="s-fb1403cfa5"></a>`module`: `stove0_core`
- <a id="s-12652b58f8"></a>`name`: `record_target_output`
- <a id="s-32ba0b188f"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-673f965494"></a>`unit`: `member`

### Declared structure

- <a id="s-1ab9b02185"></a>`kind`: `"method"`
- <a id="s-356e03a7f6"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', output: 'OutputArtifact') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-be30e08f66"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.record_target_output`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d01f6883e0b89b33b11c872c42b2974606d8d85ab09e81d25753a56f7b363282 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', output: 'OutputArtifact') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_target_output",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
