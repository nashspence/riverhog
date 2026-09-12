# gogurt_core

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core:a7d0d1dfa2 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-core` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/python/13`

## Effective policies

- `compatibility/python-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:gogurt-core` — `reference/gogurt/packages/core/src/gogurt_core/__init__.py::<module>`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `distribution` | "gogurt-core" |
| `exports` | object (32 fields) |
| `module` | "gogurt_core" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8d6b55970c60b15013cb6be9dbf5df46e20d9a05e50c37ef09b7400e91c3934 -->

```json
{
  "distribution": "gogurt-core",
  "exports": {
    "DEFAULT_GOGURT_CONFIG_FILENAME": {
      "kind": "constant",
      "value": "gogurt-routes.yaml"
    },
    "GOGURT_EMOJI": {
      "kind": "constant",
      "value": "🛹"
    },
    "GOGURT_MOUNTED_VOLUME_PROVIDER_BINDING_FORMAT": {
      "kind": "constant",
      "value": "gogurt-mounted-volume-provider-binding/v1"
    },
    "GOGURT_MOUNTED_VOLUME_PROVIDER_ENTRY_POINT_GROUP": {
      "kind": "constant",
      "value": "gogurt.mounted-volume-providers"
    },
    "GOGURT_PROVIDER_REFERENCE_FORMAT": {
      "kind": "constant",
      "value": "gogurt-provider-reference/v1"
    },
    "GOGURT_ROUTES_SCHEMA": {
      "kind": "constant",
      "value": {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "additionalProperties": false,
        "properties": {
          "kind": {
            "const": "gogurt.routes",
            "type": "string"
          },
          "routes": {
            "additionalProperties": {
              "additionalProperties": false,
              "properties": {
                "command": {
                  "items": {
                    "minLength": 1,
                    "type": "string"
                  },
                  "minItems": 1,
                  "type": "array"
                },
                "enabled": {
                  "type": "boolean"
                }
              },
              "required": [
                "command"
              ],
              "type": "object"
            },
            "propertyNames": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9-]{0,61}[a-z0-9])?$"
            },
            "type": "object"
          },
          "schema_version": {
            "const": 1,
            "type": "integer"
          }
        },
        "required": [
          "schema_version",
          "kind",
          "routes"
        ],
        "type": "object"
      }
    },
    "GOGURT_ROUTE_MARKER_FORMAT": {
      "kind": "constant",
      "value": "gogurt-route-marker/v1"
    },
    "GOGURT_ROUTE_PATTERN": {
      "kind": "constant",
      "value": "^[a-z0-9]\u0028?:[a-z0-9-]{0,61}[a-z0-9])?$"
    },
    "GogurtAction": {
      "fields": [
        {
          "default": "required",
          "name": "route",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "command",
          "type": "'tuple[str, ...]'"
        }
      ],
      "kind": "class",
      "signature": "(route: 'str', command: 'tuple[str, ...]') -> None"
    },
    "GogurtProviderKind": {
      "kind": "type-alias",
      "value": "typing.Literal['mounted-volume', 'listener-host']"
    },
    "GogurtProviderReference": {
      "fields": [
        {
          "default": "required",
          "name": "kind",
          "type": "'GogurtProviderKind'"
        },
        {
          "default": "required",
          "name": "name",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "provider_id",
          "type": "'str'"
        },
        {
          "default": "'gogurt-provider-reference/v1'",
          "name": "format",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "as_dict": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, str]'"
        },
        "from_mapping": {
          "kind": "classmethod",
          "signature": "(cls, value: 'object') -> 'GogurtProviderReference'"
        }
      },
      "signature": "(kind: 'GogurtProviderKind', name: 'str', provider_id: 'str', format: 'str' = 'gogurt-provider-reference/v1') -> None"
    },
    "GogurtRouteMarker": {
      "fields": [
        {
          "default": "required",
          "name": "route",
          "type": "'str'"
        },
        {
          "default": "'gogurt-route-marker/v1'",
          "name": "format",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "as_dict": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, str]'"
        },
        "from_mapping": {
          "kind": "classmethod",
          "signature": "(cls, value: 'object') -> 'GogurtRouteMarker'"
        }
      },
      "signature": "(route: 'str', format: 'str' = 'gogurt-route-marker/v1') -> None"
    },
    "MAX_GOGURT_INTERVAL_SECONDS": {
      "kind": "constant",
      "value": 3600
    },
    "MAX_GOGURT_MARKER_IDENTITY_CHARS": {
      "kind": "constant",
      "value": 1024
    },
    "MIN_GOGURT_INTERVAL_SECONDS": {
      "kind": "constant",
      "value": 0.1
    },
    "MountDiscovery": {
      "kind": "type-alias",
      "value": "collections.abc.Callable[[], collections.abc.Sequence[pathlib.Path]]"
    },
    "MountedMarkerObservation": {
      "fields": [
        {
          "default": "required",
          "name": "marker",
          "type": "'GogurtRouteMarker'"
        },
        {
          "default": "required",
          "name": "identity",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "signature": "(marker: 'GogurtRouteMarker', identity: 'str') -> None"
    },
    "MountedVolumeAccess": {
      "kind": "class",
      "members": {
        "discover": {
          "kind": "method",
          "signature": "(self) -> 'Sequence[Path]'"
        },
        "observe_marker": {
          "kind": "method",
          "signature": "(self, mount_point: 'Path') -> 'MountedMarkerObservation | None'"
        },
        "publish_marker": {
          "kind": "method",
          "signature": "(self, mount_point: 'Path', marker: 'GogurtRouteMarker', *, expected: 'MountedMarkerObservation | None') -> 'MountedMarkerObservation'"
        }
      },
      "signature": "(*args, **kwargs)"
    },
    "MountedVolumeProvider": {
      "kind": "class",
      "members": {
        "reference": {
          "kind": "property",
          "signature": "(self) -> 'GogurtProviderReference'"
        }
      },
      "signature": "(*args, **kwargs)"
    },
    "MountedVolumeProviderBinding": {
      "fields": [
        {
          "default": "required",
          "name": "provider_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "access",
          "type": "'MountedVolumeAccess'"
        },
        {
          "default": "'gogurt-mounted-volume-provider-binding/v1'",
          "name": "format",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "signature": "(provider_id: 'str', access: 'MountedVolumeAccess', format: 'str' = 'gogurt-mounted-volume-provider-binding/v1') -> None"
    },
    "PathInput": {
      "kind": "object",
      "type": "types.UnionType"
    },
    "default_gogurt_config_file": {
      "kind": "function",
      "signature": "(config_dir: 'PathInput', *, filename: 'str' = 'gogurt-routes.yaml') -> 'Path'"
    },
    "execute_gogurt_action": {
      "kind": "function",
      "signature": "(plan: 'Mapping[str, object]', *, provider: 'MountedVolumeProvider', capture_output: 'bool' = False) -> 'subprocess.CompletedProcess[str]'"
    },
    "iter_new_mounts": {
      "kind": "function",
      "signature": "(*, discover: 'Callable[[], Sequence[Path]]', interval_seconds: 'float' = 2.0, include_existing: 'bool' = False, sleep: 'Callable[[float], None]' = <built-in function sleep>) -> 'Iterator[Path]'"
    },
    "load_gogurt_actions": {
      "kind": "function",
      "signature": "(config_file: 'PathInput') -> 'list[GogurtAction]'"
    },
    "plan_gogurt_action": {
      "kind": "function",
      "signature": "(config_file: 'PathInput', mount_point: 'PathInput', *, provider: 'MountedVolumeProvider', actions_dir: 'PathInput | None' = None) -> 'dict[str, object]'"
    },
    "plan_gogurt_marker": {
      "kind": "function",
      "signature": "(config_file: 'PathInput', route_name: 'str', mount_point: 'PathInput', *, provider: 'MountedVolumeProvider', force: 'bool' = False) -> 'dict[str, object]'"
    },
    "revalidate_gogurt_action": {
      "kind": "function",
      "signature": "(plan: 'Mapping[str, object]', *, provider: 'MountedVolumeProvider') -> 'list[str]'"
    },
    "route_for_gogurt_marker": {
      "kind": "function",
      "signature": "(config_file: 'PathInput', route_name: 'str') -> 'str'"
    },
    "validate_gogurt_action_executables": {
      "kind": "function",
      "signature": "(config_file: 'PathInput', *, actions_dir: 'PathInput | None' = None) -> 'list[GogurtAction]'"
    },
    "validate_gogurt_interval": {
      "kind": "function",
      "signature": "(value: 'object') -> 'float'"
    },
    "write_gogurt_marker": {
      "kind": "function",
      "signature": "(config_file: 'PathInput', route_name: 'str', mount_point: 'PathInput', *, provider: 'MountedVolumeProvider', force: 'bool' = False) -> 'MountedMarkerObservation'"
    }
  },
  "module": "gogurt_core"
}
```
