# a_gogurt_windows_volume.windows_mount_points

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-volume:a-gogurt-windows-volume-windows-mount-points:f8f6c9255f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ed02dc8591"></a>
- <a id="s-3ed1ac81cc"></a>`distribution`: `a-gogurt-windows-volume`
- <a id="s-f72d8ca747"></a>`module`: `a_gogurt_windows_volume`
- <a id="s-2a48c4fdd5"></a>`name`: `windows_mount_points`
- <a id="s-6976551cf4"></a>`unit`: `export`

### Declared structure

- <a id="s-e59b472c2e"></a>`kind`: `"function"`
- <a id="s-fb43cefa1e"></a>`signature`: `"\"(logical_drives: 'Callable[[], int]' = <function _windows_logical_drive_mask>) -> 'tuple[Path, ...]'\""`

## Governing policies

- <a id="pa-c8556d242d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-volume:a_gogurt_windows_volume](../../../evidence/sources/authorities.md#src-4e8d7bf8db) — [some-implementations/gogurt/mounted-volume/windows/src/a\_gogurt\_windows\_volume/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/windows/src/a_gogurt_windows_volume/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_volume.windows_mount_points`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a8cc79bddc51251b9112a8b6324e3735b04fcb01c5c702fb4d76e0e65f6aa782 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(logical_drives: 'Callable[[], int]' = <function _windows_logical_drive_mask>) -> 'tuple[Path, ...]'\""
  },
  "distribution": "a-gogurt-windows-volume",
  "module": "a_gogurt_windows_volume",
  "name": "windows_mount_points",
  "unit": "export"
}
```

</details>
