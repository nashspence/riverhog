# gogurt_listener_runtime.start_listener

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-start-listener:4df6841140 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-198e5d5ded"></a>
- <a id="s-a2ffb0eafc"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-7a279dc9a1"></a>`module`: `gogurt_listener_runtime`
- <a id="s-36f47d54e3"></a>`name`: `start_listener`
- <a id="s-3a0da64ce3"></a>`unit`: `export`

### Declared structure

- <a id="s-2087384427"></a>`kind`: `"function"`
- <a id="s-cab47bd507"></a>`signature`: `"\"(*, paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str') -> 'dict[str, object]'\""`

## Governing policies

- <a id="pa-1bccd6ccba"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — [reference/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.start_listener`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b13a0b105d1175932b988fc1b35de60aab87f4eea030b7c2da079a268e6dcd8b -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str') -> 'dict[str, object]'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "start_listener",
  "unit": "export"
}
```

</details>
