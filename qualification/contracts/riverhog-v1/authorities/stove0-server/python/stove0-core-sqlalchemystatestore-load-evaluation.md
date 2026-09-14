# stove0_core.SqlAlchemyStateStore.load_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-load-evaluation:12984dbd0b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f32cac0b92"></a>
- <a id="s-53618d287d"></a>`distribution`: `stove0-server`
- <a id="s-281d373750"></a>`module`: `stove0_core`
- <a id="s-a907e59c04"></a>`name`: `load_evaluation`
- <a id="s-e8e6e8197d"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-7db6050897"></a>`unit`: `member`

### Declared structure

- <a id="s-dde7858eb5"></a>`kind`: `"method"`
- <a id="s-5057b2e66b"></a>`signature`: `"\"(self, evaluation_id: 'str') -> 'EvaluationRecord \| None'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-8a4b2d7ee6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.load_evaluation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 606556167bab170a9bc433214daeac5b89f4e5575b87ebae983d0a0910adfae4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str') -> 'EvaluationRecord | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_evaluation",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
