# riverhog_storage_adapter_support.parse_framed_stream

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-parse-framed-stream:f8ec4da54f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-633b4476fc"></a>
- <a id="s-238685f6c4"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-87f802a6c6"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-bebc30f3f3"></a>`name`: `parse_framed_stream`
- <a id="s-983c8fd085"></a>`unit`: `export`

### Declared structure

- <a id="s-c2dc7e8ba1"></a>`kind`: `"function"`
- <a id="s-f5cf62b09b"></a>`signature`: `"\"(chunks: 'Iterable[bytes]', model: 'type[ModelT]', *, content_length: 'int', maximum_header_bytes: 'int' = 32768) -> 'tuple[ModelT, FramedContent]'\""`

## Governing policies

- <a id="pa-8eba7a8697"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources/authorities.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.parse_framed_stream`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 130f3811cb36e1debaab4a7aaa5ab4b1c0cb83e226ff87615fe9edffb4bae06e -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(chunks: 'Iterable[bytes]', model: 'type[ModelT]', *, content_length: 'int', maximum_header_bytes: 'int' = 32768) -> 'tuple[ModelT, FramedContent]'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "parse_framed_stream",
  "unit": "export"
}
```

</details>
