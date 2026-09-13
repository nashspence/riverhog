# gogurt_linux_mounted_volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-mounted-volume:gogurt-linux-mounted-volume:3cb2ac6d96 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-mounted-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4c8ca035dd"></a>
| Field | Shape |
|---|---|
| <a id="s-cec2f2063d"></a>`candidate_id` | "python:gogurt-linux-mounted-volume:gogurt_linux_mounted_volume" |
| <a id="s-bdf9e99415"></a>`distribution` | "gogurt-linux-mounted-volume" |
| <a id="s-ee62223ccf"></a>`exports` | additional keys=`MOUNTED_VOLUME_PROVIDER_BINDING`, `discover_mount_points`, `linux_mount_points` |
| <a id="s-0be54f5b31"></a>`module` | "gogurt_linux_mounted_volume" |

## Governing policies

- <a id="pa-d000d314ce"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-linux-mounted-volume:gogurt_linux_mounted_volume](../../../evidence/sources.md#src-dfbc0b0c2f) — `reference/gogurt/mounted-volume/linux/src/gogurt_linux_mounted_volume/__init__.py`

### Machine authority

- `/external_contract/python/2`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6798eda97d4afc3822c39a5f7e13d1367f58bb0fea24b77005b5b9d6dd4cf638 -->

```json
{
  "candidate_id": "python:gogurt-linux-mounted-volume:gogurt_linux_mounted_volume",
  "distribution": "gogurt-linux-mounted-volume",
  "exports": {
    "MOUNTED_VOLUME_PROVIDER_BINDING": {
      "kind": "object",
      "type": "gogurt_core.mounts.MountedVolumeProviderBinding"
    },
    "discover_mount_points": {
      "kind": "function",
      "signature": "\"() -> 'tuple[Path, ...]'\""
    },
    "linux_mount_points": {
      "kind": "function",
      "signature": "\"(mountinfo: 'str') -> 'tuple[Path, ...]'\""
    }
  },
  "module": "gogurt_linux_mounted_volume"
}
```
