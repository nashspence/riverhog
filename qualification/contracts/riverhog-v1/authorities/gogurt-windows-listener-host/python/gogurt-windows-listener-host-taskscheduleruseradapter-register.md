# gogurt_windows_listener_host.TaskSchedulerUserAdapter.register

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-taskschedule-d60dbb298b:8c70130136 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ee5bf50747"></a>
- <a id="s-97bfb79d8a"></a>`distribution`: `gogurt-windows-listener-host`
- <a id="s-b084b19bd3"></a>`module`: `gogurt_windows_listener_host`
- <a id="s-0bf6503020"></a>`name`: `register`
- <a id="s-3472932d22"></a>`owner`: `gogurt_windows_listener_host.TaskSchedulerUserAdapter`
- <a id="s-df706186b4"></a>`unit`: `member`

### Declared structure

- <a id="s-d5c47a0122"></a>`kind`: `"method"`
- <a id="s-e6fd103559"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [TaskSchedulerUserAdapter](gogurt-windows-listener-host-taskscheduleruseradapter.md)

## Governing policies

- <a id="pa-9bca224cf3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources.md#src-ec25d3db2b) — `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.TaskSchedulerUserAdapter.register`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e4aa51431d6fb6122f69d2b996a1b013deb41461a8d04cc0c1e7f417472102c1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "register",
  "owner": "gogurt_windows_listener_host.TaskSchedulerUserAdapter",
  "unit": "member"
}
```

</details>
