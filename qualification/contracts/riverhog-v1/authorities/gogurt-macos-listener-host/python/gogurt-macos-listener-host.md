# gogurt_macos_listener_host

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-listener-host:gogurt-macos-listener-host:20bf8f4610 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-listener-host](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-cd897bea77) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-00b716397d"></a>
| Field | Shape |
|---|---|
| <a id="s-65800d62dd"></a>`candidate_id` | "python:gogurt-macos-listener-host:gogurt_macos_listener_host" |
| <a id="s-3cbc140dc4"></a>`distribution` | "gogurt-macos-listener-host" |
| <a id="s-56161af44e"></a>`exports` | additional keys=`LISTENER_HOST_PROVIDER_BINDING`, `LaunchdUserAdapter`, `default_listener_paths`, `listener_adapter`, `render_launchd_plist`, `resolve_listener_executable` |
| <a id="s-bd1d09073a"></a>`module` | "gogurt_macos_listener_host" |

## Governing policies

- <a id="pa-c302dfd452"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-macos-listener-host:gogurt_macos_listener_host](../../../evidence/sources.md#src-3a09f7fc10) — `reference/gogurt/listener-host/macos/src/gogurt_macos_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/4`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b08abaf85cc096f72b25032570ede92ce6a31eb37b5aebdd53eeb6d93d4cb89 -->

```json
{
  "candidate_id": "python:gogurt-macos-listener-host:gogurt_macos_listener_host",
  "distribution": "gogurt-macos-listener-host",
  "exports": {
    "LISTENER_HOST_PROVIDER_BINDING": {
      "kind": "object",
      "type": "gogurt_listener_runtime.platform.ListenerHostProviderBinding"
    },
    "LaunchdUserAdapter": {
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
    "render_launchd_plist": {
      "kind": "function",
      "signature": "\"(command: 'Sequence[str]') -> 'bytes'\""
    },
    "resolve_listener_executable": {
      "kind": "function",
      "signature": "\"(raw: 'str | None' = None) -> 'Path'\""
    }
  },
  "module": "gogurt_macos_listener_host"
}
```
