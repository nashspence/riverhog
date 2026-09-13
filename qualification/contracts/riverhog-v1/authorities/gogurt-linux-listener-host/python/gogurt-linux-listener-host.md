# gogurt_linux_listener_host

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host:c809221e14 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-79ad8bdd32"></a>
| Field | Shape |
|---|---|
| <a id="s-8c52fc73b3"></a>`candidate_id` | "python:gogurt-linux-listener-host:gogurt_linux_listener_host" |
| <a id="s-2bab04a373"></a>`distribution` | "gogurt-linux-listener-host" |
| <a id="s-4c9c196865"></a>`exports` | additional keys=`LISTENER_HOST_PROVIDER_BINDING`, `SystemdUserAdapter`, `default_listener_paths`, `listener_adapter`, `render_systemd_unit`, `resolve_listener_executable` |
| <a id="s-2a0e3d06de"></a>`module` | "gogurt_linux_listener_host" |

## Governing policies

- <a id="pa-f049986fac"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources.md#src-78f263d456) — `reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e7043760aa447f85ac9262234fc1c7d6462aa8f16a2201b9797d0716b4fcc024 -->

```json
{
  "candidate_id": "python:gogurt-linux-listener-host:gogurt_linux_listener_host",
  "distribution": "gogurt-linux-listener-host",
  "exports": {
    "LISTENER_HOST_PROVIDER_BINDING": {
      "kind": "object",
      "type": "gogurt_listener_runtime.platform.ListenerHostProviderBinding"
    },
    "SystemdUserAdapter": {
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
      "signature": "\"(registration_file: 'Path') -> 'None'\""
    },
    "default_listener_paths": {
      "kind": "function",
      "signature": "\"(*, environment: 'Mapping[str, str] | None' = None, home: 'Path | None' = None) -> 'ListenerRuntimePaths'\""
    },
    "listener_adapter": {
      "kind": "function",
      "signature": "\"(*, environment: 'Mapping[str, str] | None' = None, home: 'Path | None' = None) -> 'ListenerAdapter'\""
    },
    "render_systemd_unit": {
      "kind": "function",
      "signature": "\"(command: 'Sequence[str]') -> 'bytes'\""
    },
    "resolve_listener_executable": {
      "kind": "function",
      "signature": "\"(raw: 'str | None' = None) -> 'Path'\""
    }
  },
  "module": "gogurt_linux_listener_host"
}
```
