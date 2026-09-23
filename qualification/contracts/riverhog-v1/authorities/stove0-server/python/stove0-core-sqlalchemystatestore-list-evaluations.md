# stove0_core.SqlAlchemyStateStore.list_evaluations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-list-evaluations:eb58c9749e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-911a34ef00"></a>
- <a id="s-11e5a151a5"></a>`distribution`: `stove0-server`
- <a id="s-f846583c33"></a>`module`: `stove0_core`
- <a id="s-af46ca64a5"></a>`name`: `list_evaluations`
- <a id="s-025e4c0e9b"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-c6bab736c6"></a>`unit`: `member`

### Declared structure

- <a id="s-1a36d09eae"></a>`kind`: `"method"`
- <a id="s-496090b82a"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, position: 'tuple[str \| int \| bool \| bytes \| None, ...] \| None' = None, phase: 'str \| None' = None, query: 'str \| None' = None, sort: 'EvaluationSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-a03c833ae3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.list_evaluations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eca1eaf5ff24be00dfe9cb19bec80f31f73f23686053837a1998057b2ca07313 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, position: 'tuple[str | int | bool | bytes | None, ...] | None' = None, phase: 'str | None' = None, query: 'str | None' = None, sort: 'EvaluationSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'dict[str, object]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "list_evaluations",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
