# stove0_core.InMemoryWorkStore.iter_target_source_edges

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-iter-target-f4f25b75c3:9fce847e33 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-77c6fe2034"></a>
- <a id="s-fde256b02b"></a>`distribution`: `stove0-server`
- <a id="s-29e47a4a5a"></a>`module`: `stove0_core`
- <a id="s-5756572257"></a>`name`: `iter_target_source_edges`
- <a id="s-2024f2127f"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-6d95e038f3"></a>`unit`: `member`

### Declared structure

- <a id="s-ed25a9581b"></a>`kind`: `"method"`
- <a id="s-37625fbdcc"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-11aebd4eea"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.iter_target_source_edges`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1f76f3a8ca634f6d382374adc8ffab040c2ee5484919174af0ed84eb517973ca -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_source_edges",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
