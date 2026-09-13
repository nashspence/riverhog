# gogurt_path_volume_support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-path-volume-support:gogurt-path-volume-support:5be6c1f7f2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-path-volume-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7729f34ab6"></a>
| Field | Shape |
|---|---|
| <a id="s-fd2e84adac"></a>`candidate_id` | "python:gogurt-path-volume-support:gogurt_path_volume_support" |
| <a id="s-6476982873"></a>`distribution` | "gogurt-path-volume-support" |
| <a id="s-14fc046b90"></a>`exports` | additional keys=`MAX_PATH_MARKER_BYTES`, `PATH_MARKER_NAME`, `PORTABLE_MARKER_MODE`, `PathMountedVolumeAccess` |
| <a id="s-36e849b4eb"></a>`module` | "gogurt_path_volume_support" |

## Governing policies

- <a id="pa-94bce063a7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-path-volume-support:gogurt_path_volume_support](../../../evidence/sources.md#src-a67d948855) — `reference/gogurt/mounted-volume/path-support/src/gogurt_path_volume_support/__init__.py`

### Machine authority

- `/external_contract/python/6`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cb1b7005c59701b6af78f8f010a7c2309a57eeabcab3ed0d692d6e90b9a8f257 -->

```json
{
  "candidate_id": "python:gogurt-path-volume-support:gogurt_path_volume_support",
  "distribution": "gogurt-path-volume-support",
  "exports": {
    "MAX_PATH_MARKER_BYTES": {
      "kind": "constant",
      "value": 4096
    },
    "PATH_MARKER_NAME": {
      "kind": "constant",
      "value": ".gogurt"
    },
    "PORTABLE_MARKER_MODE": {
      "kind": "constant",
      "value": 420
    },
    "PathMountedVolumeAccess": {
      "fields": [
        {
          "default": "required",
          "name": "discover_mounts",
          "type": "'Callable[[], Sequence[Path]]'"
        }
      ],
      "kind": "class",
      "members": {
        "discover": {
          "kind": "method",
          "signature": "\"(self) -> 'Sequence[Path]'\""
        },
        "observe_marker": {
          "kind": "method",
          "signature": "\"(self, mount_point: 'Path') -> 'MountedMarkerObservation | None'\""
        },
        "publish_marker": {
          "kind": "method",
          "signature": "\"(self, mount_point: 'Path', document: 'GogurtRouteMarker', *, expected: 'MountedMarkerObservation | None') -> 'MountedMarkerObservation'\""
        }
      },
      "signature": "\"(discover_mounts: 'Callable[[], Sequence[Path]]') -> None\""
    }
  },
  "module": "gogurt_path_volume_support"
}
```
