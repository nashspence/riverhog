# stove0_core.SqlAlchemyStateStore.create_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-create-evaluation:b4671724a7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-386a92ae9e"></a>
- <a id="s-6e187954bb"></a>`distribution`: `stove0-server`
- <a id="s-91259facf4"></a>`module`: `stove0_core`
- <a id="s-b074fb870f"></a>`name`: `create_evaluation`
- <a id="s-ef2ad8037c"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-2eaeec78a3"></a>`unit`: `member`

### Declared structure

- <a id="s-4f1ad7d35b"></a>`kind`: `"method"`
- <a id="s-d7189cd016"></a>`signature`: `"\"(self, record: 'EvaluationRecord') -> 'EvaluationRecord'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-f08f17f5ab"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.create_evaluation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 876565828fef0768b503a6f10beb8c2b7d75c56ebca5ee487367ff404ba646e2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'EvaluationRecord') -> 'EvaluationRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create_evaluation",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
