# gogurt_windows_listener_host.TaskSchedulerUserAdapter.stop

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-taskschedule-cbc60f5163:bf211a8544 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1fbfe226f7"></a>
- <a id="s-9a5082063f"></a>`distribution`: `gogurt-windows-listener-host`
- <a id="s-c20e6ba5b8"></a>`module`: `gogurt_windows_listener_host`
- <a id="s-3f75a2e762"></a>`name`: `stop`
- <a id="s-6ff1d65fb0"></a>`owner`: `gogurt_windows_listener_host.TaskSchedulerUserAdapter`
- <a id="s-a766c7f689"></a>`unit`: `member`

### Declared structure

- <a id="s-ac7ae2db60"></a>`kind`: `"method"`
- <a id="s-e2868affc6"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [TaskSchedulerUserAdapter](gogurt-windows-listener-host-taskscheduleruseradapter.md)

## Governing policies

- <a id="pa-cc0f962315"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources/authorities.md#src-ec25d3db2b) — [reference/gogurt/listener-host/windows/src/gogurt\_windows\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.TaskSchedulerUserAdapter.stop`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 79f21e83fc1347899237526ef65b6b8ef868d82c6dcad3b556cdfa6ceeccd1c9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "stop",
  "owner": "gogurt_windows_listener_host.TaskSchedulerUserAdapter",
  "unit": "member"
}
```

</details>
