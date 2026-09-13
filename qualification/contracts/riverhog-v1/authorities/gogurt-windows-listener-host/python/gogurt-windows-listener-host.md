# gogurt_windows_listener_host

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-listener-host:gogurt-windows-listener-host:28d1f1952d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7d7a5e5d64"></a>
| Field | Shape |
|---|---|
| <a id="s-84304aa2b7"></a>`candidate_id` | "python:gogurt-windows-listener-host:gogurt_windows_listener_host" |
| <a id="s-3ca7955ee8"></a>`distribution` | "gogurt-windows-listener-host" |
| <a id="s-755a4591da"></a>`exports` | additional keys=`LISTENER_HOST_PROVIDER_BINDING`, `TaskSchedulerUserAdapter`, `WINDOWS_TASK_RESTART_COUNT`, `WINDOWS_TASK_RESTART_INTERVAL`, `WINDOWS_TASK_XML_NAMESPACE`, `default_listener_paths`, `listener_adapter`, `render_windows_task_xml`, `resolve_listener_executable`, `windows_task_name` |
| <a id="s-a43eefdee8"></a>`module` | "gogurt_windows_listener_host" |

## Governing policies

- <a id="pa-dc05704a2a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../../../evidence/sources.md#src-ec25d3db2b) — `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/7`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 26f604b8c01590992236b029975f6fc9dc616f9a2fcb3348f3b342e1ea0721c1 -->

```json
{
  "candidate_id": "python:gogurt-windows-listener-host:gogurt_windows_listener_host",
  "distribution": "gogurt-windows-listener-host",
  "exports": {
    "LISTENER_HOST_PROVIDER_BINDING": {
      "kind": "object",
      "type": "gogurt_listener_runtime.platform.ListenerHostProviderBinding"
    },
    "TaskSchedulerUserAdapter": {
      "kind": "class",
      "members": {
        "process_is_running": {
          "kind": "staticmethod",
          "signature": "\"(pid: 'int') -> 'bool'\""
        },
        "register": {
          "kind": "method",
          "signature": "\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""
        },
        "start": {
          "kind": "method",
          "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
        },
        "status": {
          "kind": "method",
          "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'\""
        },
        "stop": {
          "kind": "method",
          "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
        },
        "unregister": {
          "kind": "method",
          "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
        }
      },
      "signature": "'()'"
    },
    "WINDOWS_TASK_RESTART_COUNT": {
      "kind": "constant",
      "value": 3
    },
    "WINDOWS_TASK_RESTART_INTERVAL": {
      "kind": "constant",
      "value": "PT1M"
    },
    "WINDOWS_TASK_XML_NAMESPACE": {
      "kind": "constant",
      "value": "http://schemas.microsoft.com/windows/2004/02/mit/task"
    },
    "default_listener_paths": {
      "kind": "function",
      "signature": "\"(*, environment: 'Mapping[str, str] | None' = None, home: 'Path | None' = None) -> 'ListenerRuntimePaths'\""
    },
    "listener_adapter": {
      "kind": "function",
      "signature": "\"() -> 'ListenerAdapter'\""
    },
    "render_windows_task_xml": {
      "kind": "function",
      "signature": "\"(command: 'Sequence[str]', *, user_sid: 'str') -> 'bytes'\""
    },
    "resolve_listener_executable": {
      "kind": "function",
      "signature": "\"(raw: 'str | None' = None) -> 'Path'\""
    },
    "windows_task_name": {
      "kind": "function",
      "signature": "\"(user_sid: 'str') -> 'str'\""
    }
  },
  "module": "gogurt_windows_listener_host"
}
```
