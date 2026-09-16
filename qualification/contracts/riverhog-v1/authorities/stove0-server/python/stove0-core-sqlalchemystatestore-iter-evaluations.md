# stove0_core.SqlAlchemyStateStore.iter_evaluations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-iter-evaluations:77b20670e0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-39a4abaefa"></a>
- <a id="s-7f5b04b724"></a>`distribution`: `stove0-server`
- <a id="s-b298736144"></a>`module`: `stove0_core`
- <a id="s-35af8977b3"></a>`name`: `iter_evaluations`
- <a id="s-cadcfcd758"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-e5a6e7ab8b"></a>`unit`: `member`

### Declared structure

- <a id="s-2b8ee51fa8"></a>`kind`: `"method"`
- <a id="s-93266257dc"></a>`signature`: `"\"(self, *, phase: 'str \| None' = None, query: 'str \| None' = None, sort: 'EvaluationSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'Iterator[EvaluationRecord]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-0197b7381b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.iter_evaluations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 201ad9c92ee72d3164784cf667723dac1bb805f256b9fff086a7fb55a7071c09 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, phase: 'str | None' = None, query: 'str | None' = None, sort: 'EvaluationSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'Iterator[EvaluationRecord]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_evaluations",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
