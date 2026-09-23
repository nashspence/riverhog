# stove0_core.SqlAlchemyStateStore.iter_target_source_edges_by_input

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-iter-tar-5e036dc770:9ec72e4c85 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f5881dca02"></a>
- <a id="s-0b5b2e722c"></a>`distribution`: `stove0-server`
- <a id="s-c36317c388"></a>`module`: `stove0_core`
- <a id="s-83c07c9b4c"></a>`name`: `iter_target_source_edges_by_input`
- <a id="s-6011df6e23"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-a010b1671d"></a>`unit`: `member`

### Declared structure

- <a id="s-7ff469c440"></a>`kind`: `"method"`
- <a id="s-f9bb3484b3"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-bf583c183d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.iter_target_source_edges_by_input`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c81bd9b6d9a266291fa5b81863d68085ea7d4c54c9df17fc4f5da18da4a9bb43 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_source_edges_by_input",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
