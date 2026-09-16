# gogurt_windows_mounted_volume.discover_mount_points

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-mounted-volume:gogurt-windows-mounted-volume-discover-mount-points:3fb91272a9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-mounted-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2a51bb7ed8"></a>
- <a id="s-143e20c53d"></a>`distribution`: `gogurt-windows-mounted-volume`
- <a id="s-4fe2f671ac"></a>`module`: `gogurt_windows_mounted_volume`
- <a id="s-8723f29ab2"></a>`name`: `discover_mount_points`
- <a id="s-ea6220da38"></a>`unit`: `export`

### Declared structure

- <a id="s-b284f34e07"></a>`kind`: `"function"`
- <a id="s-29917df164"></a>`signature`: `"\"() -> 'tuple[Path, ...]'\""`

## Governing policies

- <a id="pa-4d4c59dafb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-mounted-volume:gogurt_windows_mounted_volume](../../../evidence/sources.md#src-3af4750524) — `reference/gogurt/mounted-volume/windows/src/gogurt_windows_mounted_volume/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_windows_mounted_volume.discover_mount_points`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30a90a75a4dc70db51be7120f34b3d4023c65564274d03457f46534f03dc3f49 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'tuple[Path, ...]'\""
  },
  "distribution": "gogurt-windows-mounted-volume",
  "module": "gogurt_windows_mounted_volume",
  "name": "discover_mount_points",
  "unit": "export"
}
```

</details>
