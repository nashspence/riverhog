# stove0_core.SqlAlchemyStateStore.compare_and_swap_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-compare-aa1970cb7f:2aa42b7b7c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-042606a068"></a>
- <a id="s-720310bd60"></a>`distribution`: `stove0-server`
- <a id="s-07769fbce0"></a>`module`: `stove0_core`
- <a id="s-887c3bc35d"></a>`name`: `compare_and_swap_evaluation`
- <a id="s-162c6e3ffe"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-1cb6f30ca9"></a>`unit`: `member`

### Declared structure

- <a id="s-ed6d15a847"></a>`kind`: `"method"`
- <a id="s-41b0e93ab9"></a>`signature`: `"\"(self, evaluation_id: 'str', *, expected_revision: 'int', replacement: 'EvaluationRecord') -> 'EvaluationRecord'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-0b346d4097"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.compare_and_swap_evaluation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f0a5d29d3603187013d60f5fd2b12f84418b5f999754d41a5f30e340f6f27315 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str', *, expected_revision: 'int', replacement: 'EvaluationRecord') -> 'EvaluationRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "compare_and_swap_evaluation",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
