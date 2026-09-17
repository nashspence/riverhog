# riverhog_storage_adapter_protocol.validate_write_segment_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-32393cf5ef:1fb1a62144 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f7d6c64c89"></a>
- <a id="s-375e0c86c8"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-9cd74f19aa"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-af22ab3dcb"></a>`name`: `validate_write_segment_request`
- <a id="s-340546188a"></a>`unit`: `export`

### Declared structure

- <a id="s-17bab439ab"></a>`kind`: `"function"`
- <a id="s-b3997bb51e"></a>`signature`: `"\"(request: 'WriteSegmentRequest', descriptor: 'AdapterDescriptor') -> 'None'\""`

## Governing policies

- <a id="pa-50fa06b192"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_write_segment_request`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0e11accf50fabaf17508098eb131a09f505f7f39279478cf698763dc205eda0 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'WriteSegmentRequest', descriptor: 'AdapterDescriptor') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_write_segment_request",
  "unit": "export"
}
```

</details>
