# riverhog_storage_adapter_protocol.validate_write_session_response

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-7af84bac69:7cdc34b6c6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0cb5b37259"></a>
- <a id="s-18a4a5cfa1"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-3dec63060f"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-2ea77ed422"></a>`name`: `validate_write_session_response`
- <a id="s-317087902c"></a>`unit`: `export`

### Declared structure

- <a id="s-b4b39ccd7a"></a>`kind`: `"function"`
- <a id="s-18b1a7eafe"></a>`signature`: `"\"(request: 'WriteStartRequest', response: 'WriteSession') -> 'None'\""`

## Governing policies

- <a id="pa-6c03a491a5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_write_session_response`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29657afb16e7d50796735742ca0a2ecc579260a4351ecbdc52d9cb03de6ac802 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'WriteStartRequest', response: 'WriteSession') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_write_session_response",
  "unit": "export"
}
```

</details>
