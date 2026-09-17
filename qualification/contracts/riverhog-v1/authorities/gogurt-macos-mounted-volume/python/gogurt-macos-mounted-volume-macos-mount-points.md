# gogurt_macos_mounted_volume.macos_mount_points

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-mounted-volume:gogurt-macos-mounted-volume-macos-mount-points:2246f79b17 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-mounted-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1740fa26f0"></a>
- <a id="s-6eb5217bfe"></a>`distribution`: `gogurt-macos-mounted-volume`
- <a id="s-2be7bdb251"></a>`module`: `gogurt_macos_mounted_volume`
- <a id="s-85f5df44c1"></a>`name`: `macos_mount_points`
- <a id="s-422940353a"></a>`unit`: `export`

### Declared structure

- <a id="s-8fd307e84d"></a>`kind`: `"function"`
- <a id="s-f9f427ddc6"></a>`signature`: `"\"(volumes_dir: 'Path' = PosixPath('/Volumes')) -> 'tuple[Path, ...]'\""`

## Governing policies

- <a id="pa-8c46adae14"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-macos-mounted-volume:gogurt_macos_mounted_volume](../../../evidence/sources/authorities.md#src-d5c41c5cfc) — [reference/gogurt/mounted-volume/macos/src/gogurt\_macos\_mounted\_volume/\_\_init\_\_.py](../../../../../../reference/gogurt/mounted-volume/macos/src/gogurt_macos_mounted_volume/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_macos_mounted_volume.macos_mount_points`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c766d67dd44f834e70cee6f0eed44f876a985ed8d66a0931ef28f0f12fac8eae -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(volumes_dir: 'Path' = PosixPath('/Volumes')) -> 'tuple[Path, ...]'\""
  },
  "distribution": "gogurt-macos-mounted-volume",
  "module": "gogurt_macos_mounted_volume",
  "name": "macos_mount_points",
  "unit": "export"
}
```

</details>
