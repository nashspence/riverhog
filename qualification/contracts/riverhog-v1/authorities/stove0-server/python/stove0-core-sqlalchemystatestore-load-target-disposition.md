# stove0_core.SqlAlchemyStateStore.load_target_disposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-load-tar-8ac11c2098:936c769348 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7b34ec85ed"></a>
- <a id="s-c917c8bb32"></a>`distribution`: `stove0-server`
- <a id="s-8d9a610457"></a>`module`: `stove0_core`
- <a id="s-b8fae6d5cd"></a>`name`: `load_target_disposition`
- <a id="s-1811ced41e"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-381b92879d"></a>`unit`: `member`

### Declared structure

- <a id="s-4f89f79400"></a>`kind`: `"method"`
- <a id="s-b0bbb140b3"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', input_id: 'str') -> 'InputDispositionDeclaration \| None'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-a6fad62efa"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.load_target_disposition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f51647ca7a94e38205b8ec1b9db95f9a5b8d0f72749bd28f4965d3440d28530 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', input_id: 'str') -> 'InputDispositionDeclaration | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_target_disposition",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
