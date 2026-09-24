# riverhog_storage_adapter_protocol.validate_object_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-42231c1fb1:b0c2532e64 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a8f8c14024"></a>
- <a id="s-7c0e35be8e"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-f466f3f4b2"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-0b2ab9d57c"></a>`name`: `validate_object_path`
- <a id="s-eafd10cc58"></a>`unit`: `export`

### Declared structure

- <a id="s-2f79a30a1b"></a>`kind`: `"function"`
- <a id="s-08b58f3c49"></a>`signature`: `"\"(value: 'str', *, allow_prefix: 'bool' = False) -> 'str'\""`

## Governing policies

- <a id="pa-2ef2af1c31"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_object_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d63c96202be6fef37c6d19bf82412d130bb0b0396d3c67830f9dcd0f9eb913f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str', *, allow_prefix: 'bool' = False) -> 'str'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_object_path",
  "unit": "export"
}
```

</details>
