# gogurt_windows_listener_host.TaskSchedulerUserAdapter.process_is_running

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-taskschedule-baf0857c87:1b6b65628f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2f889de298"></a>
- <a id="s-ab60efc531"></a>`distribution`: `gogurt-windows-listener-host`
- <a id="s-ca599f7c7b"></a>`module`: `gogurt_windows_listener_host`
- <a id="s-1f1706b3a8"></a>`name`: `process_is_running`
- <a id="s-b00ef5d50f"></a>`owner`: `gogurt_windows_listener_host.TaskSchedulerUserAdapter`
- <a id="s-ca86c64210"></a>`unit`: `member`

### Declared structure

- <a id="s-d05879453d"></a>`kind`: `"staticmethod"`
- <a id="s-e9f8e9158e"></a>`signature`: `"\"(pid: 'int') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [TaskSchedulerUserAdapter](gogurt-windows-listener-host-taskscheduleruseradapter.md)

## Governing policies

- <a id="pa-8c5a492770"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources.md#src-ec25d3db2b) — [reference/gogurt/listener-host/windows/src/gogurt\_windows\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.TaskSchedulerUserAdapter.process_is_running`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4265babe5c7ccd264d582afd9191cf299d38ce61d8a9014930e15c702328211e -->

```json
{
  "contract": {
    "kind": "staticmethod",
    "signature": "\"(pid: 'int') -> 'bool'\""
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "process_is_running",
  "owner": "gogurt_windows_listener_host.TaskSchedulerUserAdapter",
  "unit": "member"
}
```

</details>
