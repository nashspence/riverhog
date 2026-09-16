# gogurt_windows_mounted_volume.windows_mount_points

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-mounted-volume:gogurt-windows-mounted-volume-windows-mount-points:0599409543 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-mounted-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3232e807e4"></a>
- <a id="s-5cdcf8d8ab"></a>`distribution`: `gogurt-windows-mounted-volume`
- <a id="s-f72f10314b"></a>`module`: `gogurt_windows_mounted_volume`
- <a id="s-3c3e62176f"></a>`name`: `windows_mount_points`
- <a id="s-7c2ef8348f"></a>`unit`: `export`

### Declared structure

- <a id="s-fffa2fb22c"></a>`kind`: `"function"`
- <a id="s-054e056294"></a>`signature`: `"\"(logical_drives: 'Callable[[], int]' = <function _windows_logical_drive_mask>) -> 'tuple[Path, ...]'\""`

## Governing policies

- <a id="pa-9d5d299b95"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-mounted-volume:gogurt_windows_mounted_volume](../../../evidence/sources.md#src-3af4750524) — `reference/gogurt/mounted-volume/windows/src/gogurt_windows_mounted_volume/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_windows_mounted_volume.windows_mount_points`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8870ecb0be7cb45a30ba0cb3fadec7f726aeb9c3e2fc6c64f7316e6bae3c895f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(logical_drives: 'Callable[[], int]' = <function _windows_logical_drive_mask>) -> 'tuple[Path, ...]'\""
  },
  "distribution": "gogurt-windows-mounted-volume",
  "module": "gogurt_windows_mounted_volume",
  "name": "windows_mount_points",
  "unit": "export"
}
```

</details>
