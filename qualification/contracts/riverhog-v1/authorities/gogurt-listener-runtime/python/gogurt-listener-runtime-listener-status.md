# gogurt_listener_runtime.listener_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listener-status:6c365d0c34 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-330c7fc184"></a>
- <a id="s-86fabf2885"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-1f440cc3da"></a>`module`: `gogurt_listener_runtime`
- <a id="s-cee832be5f"></a>`name`: `listener_status`
- <a id="s-f19be714c8"></a>`unit`: `export`

### Declared structure

- <a id="s-481b6b3aac"></a>`kind`: `"function"`
- <a id="s-dfcf09c514"></a>`signature`: `"\"(*, paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str', now: 'float \| None' = None) -> 'dict[str, object]'\""`

## Governing policies

- <a id="pa-79699d2cf4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [reference/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.listener_status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4dcc834e56101543345be12be6d668e21fea856c0dbc770cb077508f3531b438 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str', now: 'float | None' = None) -> 'dict[str, object]'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "listener_status",
  "unit": "export"
}
```

</details>
