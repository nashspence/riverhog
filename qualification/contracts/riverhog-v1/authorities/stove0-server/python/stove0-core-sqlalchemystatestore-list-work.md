# stove0_core.SqlAlchemyStateStore.list_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-list-work:01ea9bce08 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d5208bc61d"></a>
- <a id="s-75bb39f7f0"></a>`distribution`: `stove0-server`
- <a id="s-d7c6d61359"></a>`module`: `stove0_core`
- <a id="s-1cec50a8bd"></a>`name`: `list_work`
- <a id="s-e53c3ad605"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-7aa8e3fcaa"></a>`unit`: `member`

### Declared structure

- <a id="s-2cfba3dfbd"></a>`kind`: `"method"`
- <a id="s-46323fab61"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, position: 'tuple[str \| int \| bool \| bytes \| None, ...] \| None' = None, phase: 'str \| None' = None, query: 'str \| None' = None, sort: 'WorkSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-cf7ff2a4e4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.list_work`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39c6c76611c2eb3ac8a9f0a53f9c3d428aab83c44bb12f656975c4b15bff99ef -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, position: 'tuple[str | int | bool | bytes | None, ...] | None' = None, phase: 'str | None' = None, query: 'str | None' = None, sort: 'WorkSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'dict[str, object]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "list_work",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
