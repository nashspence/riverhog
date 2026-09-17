# riverhog_storage_adapter_protocol.validate_write_start_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-ebf801c8fc:423d700177 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ba0ff5683f"></a>
- <a id="s-b14cbb0a4f"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-f7c4efe5df"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-833ec30210"></a>`name`: `validate_write_start_request`
- <a id="s-5c6dfdf353"></a>`unit`: `export`

### Declared structure

- <a id="s-5886d6895f"></a>`kind`: `"function"`
- <a id="s-14af61382c"></a>`signature`: `"\"(request: 'WriteStartRequest', descriptor: 'AdapterDescriptor') -> 'None'\""`

## Governing policies

- <a id="pa-b361e40bec"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_write_start_request`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2f8e28f4fd78a1968e59d458bc76263d860a32c75fbb2c504ac3c658275f626 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'WriteStartRequest', descriptor: 'AdapterDescriptor') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_write_start_request",
  "unit": "export"
}
```

</details>
