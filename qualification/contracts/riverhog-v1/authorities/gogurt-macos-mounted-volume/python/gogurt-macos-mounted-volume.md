# gogurt_macos_mounted_volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-mounted-volume:gogurt-macos-mounted-volume:71df7a7704 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-mounted-volume](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4a5160e00f"></a>
| Field | Shape |
|---|---|
| <a id="s-3df232ad44"></a>`candidate_id` | "python:gogurt-macos-mounted-volume:gogurt_macos_mounted_volume" |
| <a id="s-e6371e3863"></a>`distribution` | "gogurt-macos-mounted-volume" |
| <a id="s-c8f3f6e535"></a>`exports` | additional keys=`MOUNTED_VOLUME_PROVIDER_BINDING`, `discover_mount_points`, `macos_mount_points` |
| <a id="s-33f0043144"></a>`module` | "gogurt_macos_mounted_volume" |

## Governing policies

- <a id="pa-dc04b5670f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-macos-mounted-volume:gogurt_macos_mounted_volume](../../../evidence/sources.md#src-d5c41c5cfc) — `reference/gogurt/mounted-volume/macos/src/gogurt_macos_mounted_volume/__init__.py`

### Machine authority

- `/external_contract/python/5`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa2fbd0f3e0c83cc6b70347471428517ebd2a6d43506766c06679d55daa89d53 -->

```json
{
  "candidate_id": "python:gogurt-macos-mounted-volume:gogurt_macos_mounted_volume",
  "distribution": "gogurt-macos-mounted-volume",
  "exports": {
    "MOUNTED_VOLUME_PROVIDER_BINDING": {
      "kind": "object",
      "type": "gogurt_core.mounts.MountedVolumeProviderBinding"
    },
    "discover_mount_points": {
      "kind": "function",
      "signature": "\"() -> 'tuple[Path, ...]'\""
    },
    "macos_mount_points": {
      "kind": "function",
      "signature": "\"(volumes_dir: 'Path' = PosixPath('/Volumes')) -> 'tuple[Path, ...]'\""
    }
  },
  "module": "gogurt_macos_mounted_volume"
}
```
