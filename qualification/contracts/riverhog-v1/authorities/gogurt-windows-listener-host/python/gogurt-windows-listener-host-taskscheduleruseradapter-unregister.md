# gogurt_windows_listener_host.TaskSchedulerUserAdapter.unregister

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host-taskschedule-a8320b85bb:f7fb0048af -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0d7f362284"></a>
| Field | Shape |
|---|---|
| <a id="s-1f82f3e8ed"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-e76c4c3c5e"></a>`distribution` | "gogurt-windows-listener-host" |
| <a id="s-28c79c377d"></a>`module` | "gogurt_windows_listener_host" |
| <a id="s-b0166afd7a"></a>`name` | "unregister" |
| <a id="s-1b59ac2923"></a>`owner` | "gogurt_windows_listener_host.TaskSchedulerUserAdapter" |
| <a id="s-70ee390aeb"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [gogurt_windows_listener_host.TaskSchedulerUserAdapter](gogurt-windows-listener-host-taskscheduleruseradapter.md)

## Governing policies

- <a id="pa-758b9e8814"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources.md#src-ec25d3db2b) — `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_windows_listener_host.TaskSchedulerUserAdapter.unregister`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fe706a7aec53f91fa228233b89caa3195056de2da74af862a2e41e58e1c9f6a2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "gogurt-windows-listener-host",
  "module": "gogurt_windows_listener_host",
  "name": "unregister",
  "owner": "gogurt_windows_listener_host.TaskSchedulerUserAdapter",
  "unit": "member"
}
```
