# gogurt_windows_listener_host.TaskSchedulerUserAdapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-taskschedule-071715347c:414db27ecd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7d98801104"></a>
- <a id="s-472557ee5d"></a>`distribution`: `gogurt-windows-listener-host`
- <a id="s-52a2b1d632"></a>`module`: `gogurt_windows_listener_host`
- <a id="s-5eb62d79c7"></a>`name`: `TaskSchedulerUserAdapter`
- <a id="s-25c9444cea"></a>`unit`: `export`

### Declared structure

- <a id="s-82ffd4f479"></a>`kind`: `"class"`
- <a id="s-d9009422ff"></a>`signature`: `"'()'"`

## Maintained corroboration

### Related interface records

- [status](gogurt-windows-listener-host-taskscheduleruseradapter-status.md)
- [start](gogurt-windows-listener-host-taskscheduleruseradapter-start.md)
- [unregister](gogurt-windows-listener-host-taskscheduleruseradapter-unregister.md)
- [process_is_running](gogurt-windows-listener-host-taskscheduleruseradapter-process-is-running.md)
- [stop](gogurt-windows-listener-host-taskscheduleruseradapter-stop.md)
- [register](gogurt-windows-listener-host-taskscheduleruseradapter-register.md)

## Governing policies

- <a id="pa-c2c383a0fc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources.md#src-ec25d3db2b) — `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.TaskSchedulerUserAdapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c774022cb7e049a63d6a4dd9fbf847e7665fff5ad035cea10061dfe0c3e0137 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'()'"
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "TaskSchedulerUserAdapter",
  "unit": "export"
}
```

</details>
