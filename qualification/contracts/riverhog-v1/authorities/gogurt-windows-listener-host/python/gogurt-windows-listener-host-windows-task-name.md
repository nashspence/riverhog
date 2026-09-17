# gogurt_windows_listener_host.windows_task_name

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-windows-task-name:e2e5a518e5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1699a26048"></a>
- <a id="s-3c44680126"></a>`distribution`: `gogurt-windows-listener-host`
- <a id="s-c72740ad84"></a>`module`: `gogurt_windows_listener_host`
- <a id="s-7bfd5ff6a3"></a>`name`: `windows_task_name`
- <a id="s-4650baa130"></a>`unit`: `export`

### Declared structure

- <a id="s-15a7d8d368"></a>`kind`: `"function"`
- <a id="s-5f0d2d44e0"></a>`signature`: `"\"(user_sid: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-04ad3a0a24"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources/authorities.md#src-ec25d3db2b) — [reference/gogurt/listener-host/windows/src/gogurt\_windows\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.windows_task_name`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b090901b6c40ec54466722b2c2c9de8a2d3089a9c519fe31331b71581e3bf21f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(user_sid: 'str') -> 'str'\""
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "windows_task_name",
  "unit": "export"
}
```

</details>
