# riverhog_storage_adapter_protocol.validate_write_completion_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-17b36214ad:11f1c399ca -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fd3258e513"></a>
- <a id="s-fdf176740a"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-5f02d1d040"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-0654370e7c"></a>`name`: `validate_write_completion_request`
- <a id="s-058cce421f"></a>`unit`: `export`

### Declared structure

- <a id="s-3b29082b93"></a>`kind`: `"function"`
- <a id="s-f8a97426b7"></a>`signature`: `"\"(request: 'WriteCompleteRequest', descriptor: 'AdapterDescriptor') -> 'None'\""`

## Governing policies

- <a id="pa-54c8c2e690"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_write_completion_request`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d25c5c7dde50eb6bdff0092d394eb6c340a640697c159a83d8d391c21e65f92 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'WriteCompleteRequest', descriptor: 'AdapterDescriptor') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_write_completion_request",
  "unit": "export"
}
```

</details>
