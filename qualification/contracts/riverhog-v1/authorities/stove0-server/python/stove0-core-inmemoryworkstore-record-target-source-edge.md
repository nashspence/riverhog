# stove0_core.InMemoryWorkStore.record_target_source_edge

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-record-targ-ef2df918f3:73ac13c9f9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-03d77d1359"></a>
- <a id="s-f90a956ced"></a>`distribution`: `stove0-server`
- <a id="s-6528b35831"></a>`module`: `stove0_core`
- <a id="s-7cd3c99feb"></a>`name`: `record_target_source_edge`
- <a id="s-fc4b3ba800"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-ec087f4ab0"></a>`unit`: `member`

### Declared structure

- <a id="s-63531b2fab"></a>`kind`: `"method"`
- <a id="s-bc7284bee9"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', edge: 'OutputSourceEdge') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-a6ea9b09f0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.record_target_source_edge`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc3fe7e5645ec02b5cb47d25ea2e33dd64160adf4eb4aedb5d4f1cbeeaa7adb6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', edge: 'OutputSourceEdge') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_target_source_edge",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
