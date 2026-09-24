# stove0_core.Stove0RiverhogClient.begin_source_collection_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-begin-so-ef41a68cec:920298fb8f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-412919c673"></a>
- <a id="s-3919027fa2"></a>`distribution`: `stove0-server`
- <a id="s-1a251bae74"></a>`module`: `stove0_core`
- <a id="s-1aa4c493df"></a>`name`: `begin_source_collection_retirement`
- <a id="s-441c493221"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-e07548e4d7"></a>`unit`: `member`

### Declared structure

- <a id="s-1435ef1dbf"></a>`kind`: `"method"`
- <a id="s-111636b768"></a>`signature`: `"\"(self, record: 'WorkRecord') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-697a6765c6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.begin_source_collection_retirement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82d4902450874e321200a1523069d5269cb516917ac7fa07a849da78cfeff664 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord') -> 'bool'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "begin_source_collection_retirement",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```

</details>
