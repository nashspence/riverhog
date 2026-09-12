# gogurt_listener_runtime

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime:5c2769595e -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-listener-runtime` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/python/14`

## Effective policies

- `compatibility/python-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:gogurt-listener-runtime` — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py::<module>`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `distribution` | "gogurt-listener-runtime" |
| `exports` | object (33 fields) |
| `module` | "gogurt_listener_runtime" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 41173d54de2d90b1dad2313e619dfea4cea4cd43eb9eb333af8dfdc67a9d6b68 -->

```json
{
  "distribution": "gogurt-listener-runtime",
  "exports": {
    "GOGURT_LISTENER_HOST_PROVIDER_BINDING_FORMAT": {
      "kind": "constant",
      "value": "gogurt-listener-host-provider-binding/v1"
    },
    "GOGURT_LISTENER_HOST_PROVIDER_ENTRY_POINT_GROUP": {
      "kind": "constant",
      "value": "gogurt.listener-host-providers"
    },
    "LISTENER_CONFIG_SCHEMA": {
      "kind": "constant",
      "value": "gogurt-listener-config/v1"
    },
    "LISTENER_HEARTBEAT_SCHEMA": {
      "kind": "constant",
      "value": "gogurt-listener-heartbeat/v1"
    },
    "LISTENER_OPERATIONS": {
      "kind": "constant",
      "value": [
        "install",
        "status",
        "start",
        "stop",
        "restart",
        "uninstall"
      ]
    },
    "LISTENER_STATE_SCHEMA": {
      "kind": "constant",
      "value": 1
    },
    "LISTENER_STATUS_SCHEMA": {
      "kind": "constant",
      "value": "gogurt-listener-status/v1"
    },
    "ListenerAdapter": {
      "kind": "class",
      "members": {
        "process_is_running": {
          "kind": "method",
          "signature": "(self, pid: 'int') -> 'bool'"
        },
        "register": {
          "kind": "method",
          "signature": "(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'"
        },
        "start": {
          "kind": "method",
          "signature": "(self, paths: 'ListenerRuntimePaths') -> 'None'"
        },
        "status": {
          "kind": "method",
          "signature": "(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'"
        },
        "stop": {
          "kind": "method",
          "signature": "(self, paths: 'ListenerRuntimePaths') -> 'None'"
        },
        "unregister": {
          "kind": "method",
          "signature": "(self, paths: 'ListenerRuntimePaths') -> 'None'"
        }
      },
      "signature": "(*args, **kwargs)"
    },
    "ListenerConfig": {
      "fields": [
        {
          "default": "required",
          "name": "executable",
          "type": "'Path'"
        },
        {
          "default": "required",
          "name": "routes_file",
          "type": "'Path'"
        },
        {
          "default": "required",
          "name": "actions_dir",
          "type": "'Path | None'"
        },
        {
          "default": "required",
          "name": "interval_seconds",
          "type": "'float'"
        },
        {
          "default": "required",
          "name": "state_dir",
          "type": "'Path'"
        },
        {
          "default": "required",
          "name": "mounted_volume_provider",
          "type": "'GogurtProviderReference'"
        },
        {
          "default": "required",
          "name": "listener_host_provider",
          "type": "'GogurtProviderReference'"
        },
        {
          "default": "True",
          "name": "autorun",
          "type": "'bool'"
        }
      ],
      "kind": "class",
      "members": {
        "content": {
          "kind": "method",
          "signature": "(self) -> 'bytes'"
        },
        "payload": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        },
        "read": {
          "kind": "classmethod",
          "signature": "(cls, path: 'Path') -> 'ListenerConfig'"
        },
        "write": {
          "kind": "method",
          "signature": "(self, path: 'Path') -> 'None'"
        }
      },
      "signature": "(executable: 'Path', routes_file: 'Path', actions_dir: 'Path | None', interval_seconds: 'float', state_dir: 'Path', mounted_volume_provider: 'GogurtProviderReference', listener_host_provider: 'GogurtProviderReference', autorun: 'bool' = True) -> None"
    },
    "ListenerError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "ListenerHostProviderBinding": {
      "fields": [
        {
          "default": "required",
          "name": "provider_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "paths",
          "type": "'Callable[[], ListenerRuntimePaths]'"
        },
        {
          "default": "required",
          "name": "adapter",
          "type": "'Callable[[], ListenerAdapter]'"
        },
        {
          "default": "required",
          "name": "executable",
          "type": "'Callable[[str | None], Path]'"
        },
        {
          "default": "'gogurt-listener-host-provider-binding/v1'",
          "name": "format",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "signature": "(provider_id: 'str', paths: 'Callable[[], ListenerRuntimePaths]', adapter: 'Callable[[], ListenerAdapter]', executable: 'Callable[[str | None], Path]', format: 'str' = 'gogurt-listener-host-provider-binding/v1') -> None"
    },
    "ListenerLock": {
      "kind": "class",
      "signature": "(path: 'Path') -> 'None'"
    },
    "ListenerPlatformError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "ListenerRuntime": {
      "kind": "class",
      "members": {
        "request_stop": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "run": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "run_once": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        }
      },
      "signature": "(config: 'ListenerConfig', paths: 'ListenerRuntimePaths', *, mounted_volume_provider: 'MountedVolumeProvider', product_version: 'str', clock: 'Callable[[], float]' = <built-in function time>, sleep: 'Callable[[float], None]' = <built-in function sleep>, logger: 'logging.Logger | None' = None) -> 'None'"
    },
    "ListenerRuntimePaths": {
      "fields": [
        {
          "default": "required",
          "name": "state_dir",
          "type": "'Path'"
        },
        {
          "default": "required",
          "name": "config_file",
          "type": "'Path'"
        },
        {
          "default": "required",
          "name": "database_file",
          "type": "'Path'"
        },
        {
          "default": "required",
          "name": "heartbeat_file",
          "type": "'Path'"
        },
        {
          "default": "required",
          "name": "lock_file",
          "type": "'Path'"
        },
        {
          "default": "required",
          "name": "log_file",
          "type": "'Path'"
        },
        {
          "default": "required",
          "name": "stop_file",
          "type": "'Path'"
        }
      ],
      "kind": "class",
      "signature": "(state_dir: 'Path', config_file: 'Path', database_file: 'Path', heartbeat_file: 'Path', lock_file: 'Path', log_file: 'Path', stop_file: 'Path') -> None"
    },
    "ListenerStore": {
      "kind": "class",
      "members": {
        "create": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "finish_dispatch": {
          "kind": "method",
          "signature": "(self, dispatch_id: 'str', *, return_code: 'int | None', error: 'str | None', uncertain: 'bool' = False, now: 'float') -> 'str'"
        },
        "mark_running_uncertain": {
          "kind": "method",
          "signature": "(self, dispatch_id: 'str', *, error: 'str', now: 'float') -> 'None'"
        },
        "observe": {
          "kind": "method",
          "signature": "(self, mount_points: 'Sequence[Path]', planner: 'Callable[[Path], Mapping[str, object]]', *, now: 'float') -> 'list[str]'"
        },
        "runnable": {
          "kind": "method",
          "signature": "(self, *, now: 'float', limit: 'int') -> 'list[str]'"
        },
        "start_dispatch": {
          "kind": "method",
          "signature": "(self, dispatch_id: 'str', *, now: 'float') -> 'dict[str, object] | None'"
        },
        "summary": {
          "kind": "method",
          "signature": "(self, *, timeout_seconds: 'float' = 30) -> 'dict[str, object]'"
        }
      },
      "signature": "(path: 'Path') -> 'None'"
    },
    "NativeListenerStatus": {
      "fields": [
        {
          "default": "required",
          "name": "installed",
          "type": "'bool'"
        },
        {
          "default": "required",
          "name": "enabled",
          "type": "'bool'"
        },
        {
          "default": "required",
          "name": "running",
          "type": "'bool'"
        }
      ],
      "kind": "class",
      "signature": "(installed: 'bool', enabled: 'bool', running: 'bool') -> None"
    },
    "PRIVATE_DIRECTORY_MODE": {
      "kind": "constant",
      "value": 448
    },
    "PRIVATE_FILE_MODE": {
      "kind": "constant",
      "value": 384
    },
    "atomic_write": {
      "kind": "function",
      "signature": "(destination: 'Path', content: 'bytes', *, mode: 'int') -> 'None'"
    },
    "ensure_private_directory": {
      "kind": "function",
      "signature": "(path: 'Path') -> 'None'"
    },
    "ensure_private_file": {
      "kind": "function",
      "signature": "(path: 'Path') -> 'None'"
    },
    "ensure_private_files": {
      "kind": "function",
      "signature": "(paths: 'Iterable[Path]') -> 'None'"
    },
    "install_listener": {
      "kind": "function",
      "signature": "(routes_file: 'Path', *, actions_dir: 'Path | None', interval_seconds: 'float' = 2.0, executable: 'Path', paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str', mounted_volume_provider: 'GogurtProviderReference', listener_host_provider: 'GogurtProviderReference', wait_for_health: 'bool' = True) -> 'dict[str, object]'"
    },
    "listener_status": {
      "kind": "function",
      "signature": "(*, paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str', now: 'float | None' = None) -> 'dict[str, object]'"
    },
    "open_private_text_append": {
      "kind": "function",
      "signature": "(path: 'Path', *, encoding: 'str', errors: 'str | None') -> 'TextIO'"
    },
    "promote_staged": {
      "kind": "function",
      "signature": "(temporary: 'Path', destination: 'Path', *, mode: 'int') -> 'None'"
    },
    "restart_listener": {
      "kind": "function",
      "signature": "(*, paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str') -> 'dict[str, object]'"
    },
    "run_listener": {
      "kind": "function",
      "signature": "(config_file: 'Path', *, mounted_volume_provider: 'MountedVolumeProvider', product_version: 'str') -> 'None'"
    },
    "stage_bytes": {
      "kind": "function",
      "signature": "(destination: 'Path', content: 'bytes', *, mode: 'int') -> 'Path'"
    },
    "start_listener": {
      "kind": "function",
      "signature": "(*, paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str') -> 'dict[str, object]'"
    },
    "stop_listener": {
      "kind": "function",
      "signature": "(*, paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str') -> 'dict[str, object]'"
    },
    "uninstall_listener": {
      "kind": "function",
      "signature": "(*, paths: 'ListenerRuntimePaths', adapter: 'ListenerAdapter', product_version: 'str') -> 'dict[str, object]'"
    }
  },
  "module": "gogurt_listener_runtime"
}
```
