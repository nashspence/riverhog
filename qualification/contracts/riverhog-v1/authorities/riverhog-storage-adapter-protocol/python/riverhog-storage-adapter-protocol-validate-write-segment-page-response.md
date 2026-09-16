# riverhog_storage_adapter_protocol.validate_write_segment_page_response

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-4890596912:32844c3c92 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e272e18d07"></a>
- <a id="s-986bedf8d3"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-63daf7e341"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-f8529b89b4"></a>`name`: `validate_write_segment_page_response`
- <a id="s-10ae69402d"></a>`unit`: `export`

### Declared structure

- <a id="s-4093f0e9b1"></a>`kind`: `"function"`
- <a id="s-50e98b7da9"></a>`signature`: `"\"(request: 'WriteSegmentListRequest', response: 'WriteSegmentPage', descriptor: 'AdapterDescriptor') -> 'None'\""`

## Governing policies

- <a id="pa-e1ecf9ba02"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_write_segment_page_response`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40c168dba8bd45bfff1bafe6c2ff32e236cbd83a4572032caa10b1a7eeb917c7 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'WriteSegmentListRequest', response: 'WriteSegmentPage', descriptor: 'AdapterDescriptor') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_write_segment_page_response",
  "unit": "export"
}
```

</details>
