# gogurt_windows_listener_host.TaskSchedulerUserAdapter.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-taskschedule-1b951bbdc9:ff6a04469f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e7ebdc2150"></a>
- <a id="s-68f4897049"></a>`distribution`: `gogurt-windows-listener-host`
- <a id="s-e22e8202ab"></a>`module`: `gogurt_windows_listener_host`
- <a id="s-3940577b82"></a>`name`: `status`
- <a id="s-8dd2b6a98b"></a>`owner`: `gogurt_windows_listener_host.TaskSchedulerUserAdapter`
- <a id="s-46424bde97"></a>`unit`: `member`

### Declared structure

- <a id="s-c84d8fedb7"></a>`kind`: `"method"`
- <a id="s-9141fbc3ea"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'\""`

## Maintained corroboration

### Related interface records

- [TaskSchedulerUserAdapter](gogurt-windows-listener-host-taskscheduleruseradapter.md)

## Governing policies

- <a id="pa-124b81c6c0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources.md#src-ec25d3db2b) — `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.TaskSchedulerUserAdapter.status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0cc771c258e7461f123b103255d087e805ad27c5820fc06075b70b3d77263ab1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'\""
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "status",
  "owner": "gogurt_windows_listener_host.TaskSchedulerUserAdapter",
  "unit": "member"
}
```

</details>
