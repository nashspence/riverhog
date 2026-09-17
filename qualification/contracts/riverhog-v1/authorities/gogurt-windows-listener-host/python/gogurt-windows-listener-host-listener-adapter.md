# gogurt_windows_listener_host.listener_adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-listener-adapter:fab71002fc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a0fc05a568"></a>
- <a id="s-de899bc05e"></a>`distribution`: `gogurt-windows-listener-host`
- <a id="s-8c3d1f711d"></a>`module`: `gogurt_windows_listener_host`
- <a id="s-845c0ffa02"></a>`name`: `listener_adapter`
- <a id="s-9bdc0046b7"></a>`unit`: `export`

### Declared structure

- <a id="s-5a1278e360"></a>`kind`: `"function"`
- <a id="s-fbfedac992"></a>`signature`: `"\"() -> 'ListenerAdapter'\""`

## Governing policies

- <a id="pa-93db8c3af4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources/authorities.md#src-ec25d3db2b) — [reference/gogurt/listener-host/windows/src/gogurt\_windows\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.listener_adapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55128621ec23fc7ce72afdeefcf8e65b5daee83a2070d2f7fa1dd2aea7422547 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'ListenerAdapter'\""
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "listener_adapter",
  "unit": "export"
}
```

</details>
