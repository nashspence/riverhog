# riverhog_storage_adapter_support.framed_body_length

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-framed-body-length:b5581640e6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e9178c842"></a>
- <a id="s-fe97dab093"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-55ce39055b"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-722a037ccc"></a>`name`: `framed_body_length`
- <a id="s-f10c54a0f3"></a>`unit`: `export`

### Declared structure

- <a id="s-92ad956df9"></a>`kind`: `"function"`
- <a id="s-36057a1f31"></a>`signature`: `"\"(model: 'BaseModel') -> 'int'\""`

## Governing policies

- <a id="pa-51dbc71614"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources/authorities.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.framed_body_length`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3b64b6f65dce27afce2f6f29e6d70fb328eddc7e4678485b4728225afada1672 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(model: 'BaseModel') -> 'int'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "framed_body_length",
  "unit": "export"
}
```

</details>
