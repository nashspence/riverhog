# gogurt_windows_mounted_volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-mounted-volume:gogurt-windows-mounted-volume:bf82fabd47 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-mounted-volume](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-514dc2fee8"></a>
| Field | Shape |
|---|---|
| <a id="s-42429546ca"></a>`candidate_id` | "python:gogurt-windows-mounted-volume:gogurt_windows_mounted_volume" |
| <a id="s-6c09fc8a98"></a>`distribution` | "gogurt-windows-mounted-volume" |
| <a id="s-b0493cef02"></a>`exports` | additional keys=`MOUNTED_VOLUME_PROVIDER_BINDING`, `discover_mount_points`, `windows_mount_points` |
| <a id="s-624dd57f6b"></a>`module` | "gogurt_windows_mounted_volume" |

## Governing policies

- <a id="pa-beac961a72"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-mounted-volume:gogurt_windows_mounted_volume](../../../evidence/sources.md#src-3af4750524) — `reference/gogurt/mounted-volume/windows/src/gogurt_windows_mounted_volume/__init__.py`

### Machine authority

- `/external_contract/python/8`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1dcc834ca71a33c07f4af504ed7bbf74712067cdde87bf956ace5952fd42782c -->

```json
{
  "candidate_id": "python:gogurt-windows-mounted-volume:gogurt_windows_mounted_volume",
  "distribution": "gogurt-windows-mounted-volume",
  "exports": {
    "MOUNTED_VOLUME_PROVIDER_BINDING": {
      "kind": "object",
      "type": "gogurt_core.mounts.MountedVolumeProviderBinding"
    },
    "discover_mount_points": {
      "kind": "function",
      "signature": "\"() -> 'tuple[Path, ...]'\""
    },
    "windows_mount_points": {
      "kind": "function",
      "signature": "\"(logical_drives: 'Callable[[], int]' = <function _windows_logical_drive_mask>) -> 'tuple[Path, ...]'\""
    }
  },
  "module": "gogurt_windows_mounted_volume"
}
```
