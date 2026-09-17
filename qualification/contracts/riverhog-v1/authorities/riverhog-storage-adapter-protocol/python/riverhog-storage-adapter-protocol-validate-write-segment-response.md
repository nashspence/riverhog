# riverhog_storage_adapter_protocol.validate_write_segment_response

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-bb9882df9f:4d15feddac -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e46d4d9ff5"></a>
- <a id="s-2579570447"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-57f4c1bf09"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-4f140c6634"></a>`name`: `validate_write_segment_response`
- <a id="s-1fb80afd16"></a>`unit`: `export`

### Declared structure

- <a id="s-b2bc2d113d"></a>`kind`: `"function"`
- <a id="s-c40f94cc71"></a>`signature`: `"\"(request: 'WriteSegmentRequest', response: 'WriteSegmentReceipt') -> 'None'\""`

## Governing policies

- <a id="pa-b2a9be24ce"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_write_segment_response`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d1a8419e19b4669a6c1de7308dd58e643290d2e6e9ceb3d19f40eff01275a35 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'WriteSegmentRequest', response: 'WriteSegmentReceipt') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_write_segment_response",
  "unit": "export"
}
```

</details>
