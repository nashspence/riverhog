# stove0_core.Stove0RiverhogClient.retire_input

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-retire-input:41e2016de0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-281f9845c7"></a>
- <a id="s-23dc60ad81"></a>`distribution`: `stove0-server`
- <a id="s-527336ef88"></a>`module`: `stove0_core`
- <a id="s-900e670609"></a>`name`: `retire_input`
- <a id="s-50500cdf56"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-ae9a7d150f"></a>`unit`: `member`

### Declared structure

- <a id="s-a919a32258"></a>`kind`: `"method"`
- <a id="s-3a187760f5"></a>`signature`: `"\"(self, record: 'WorkRecord', collection_id: 'int') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-4e81814322"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.retire_input`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 53830f502d99e26ca010fbe793fe174b9002d58bf8effa136d1a15efa99915f3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord', collection_id: 'int') -> 'bool'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retire_input",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```

</details>
