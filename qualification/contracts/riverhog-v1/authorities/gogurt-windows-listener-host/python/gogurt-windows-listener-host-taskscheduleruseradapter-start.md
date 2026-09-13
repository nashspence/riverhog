# gogurt_windows_listener_host.TaskSchedulerUserAdapter.start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-taskschedule-9ac1475aad:15da22a6bb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31c870addf"></a>
| Field | Shape |
|---|---|
| <a id="s-909c96a993"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-70ce7079ac"></a>`distribution` | "gogurt-windows-listener-host" |
| <a id="s-f52cdd5247"></a>`module` | "gogurt_windows_listener_host" |
| <a id="s-973691462c"></a>`name` | "start" |
| <a id="s-10e595a836"></a>`owner` | "gogurt_windows_listener_host.TaskSchedulerUserAdapter" |
| <a id="s-eb3bff6a49"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [gogurt_windows_listener_host.TaskSchedulerUserAdapter](gogurt-windows-listener-host-taskscheduleruseradapter.md)

## Governing policies

- <a id="pa-383ba1d31b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources.md#src-ec25d3db2b) — `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.TaskSchedulerUserAdapter.start`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a39f60aedb33db04a8802e0cb0fa9a16b31f1ad21648ef7e45484274266c3f2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "start",
  "owner": "gogurt_windows_listener_host.TaskSchedulerUserAdapter",
  "unit": "member"
}
```
