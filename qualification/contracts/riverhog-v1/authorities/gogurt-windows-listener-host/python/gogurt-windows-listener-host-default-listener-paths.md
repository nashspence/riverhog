# gogurt_windows_listener_host.default_listener_paths

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-default-listener-paths:bcdf2b69ec -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0b840a4d9f"></a>
- <a id="s-1fdb2ab992"></a>`distribution`: `gogurt-windows-listener-host`
- <a id="s-57f7932c3a"></a>`module`: `gogurt_windows_listener_host`
- <a id="s-910f27e032"></a>`name`: `default_listener_paths`
- <a id="s-b94d1de694"></a>`unit`: `export`

### Declared structure

- <a id="s-ba380d4bc1"></a>`kind`: `"function"`
- <a id="s-2c808216fd"></a>`signature`: `"\"(*, environment: 'Mapping[str, str] \| None' = None, home: 'Path \| None' = None) -> 'ListenerRuntimePaths'\""`

## Governing policies

- <a id="pa-78a1dc456d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources/authorities.md#src-ec25d3db2b) — [reference/gogurt/listener-host/windows/src/gogurt\_windows\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.default_listener_paths`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d1fa4bbbf8f6ca18443275b22004d9c47e766dab82fda1e512284e32952d2a63 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, environment: 'Mapping[str, str] | None' = None, home: 'Path | None' = None) -> 'ListenerRuntimePaths'\""
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "default_listener_paths",
  "unit": "export"
}
```

</details>
