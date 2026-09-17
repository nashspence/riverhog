# gogurt_listener_runtime.stop_listener

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-stop-listener:1e7cd4829c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-502327069f"></a>
- <a id="s-216d41bca0"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-0276061110"></a>`module`: `gogurt_listener_runtime`
- <a id="s-af814a91f5"></a>`name`: `stop_listener`
- <a id="s-3ae77434c7"></a>`unit`: `export`

### Declared structure

- <a id="s-864727c0c6"></a>`kind`: `"function"`
- <a id="s-b235e8f0e0"></a>`signature`: `"\"(*, paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str') -> 'dict[str, object]'\""`

## Governing policies

- <a id="pa-8c1a7e7146"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [reference/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.stop_listener`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a90aedc5b51eb4b0a196fb826eaa5c32e95ba7c762121ac5d75b0fb9398cd23 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str') -> 'dict[str, object]'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "stop_listener",
  "unit": "export"
}
```

</details>
