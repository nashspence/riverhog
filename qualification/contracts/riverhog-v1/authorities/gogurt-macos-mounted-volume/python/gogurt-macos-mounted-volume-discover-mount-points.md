# gogurt_macos_mounted_volume.discover_mount_points

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-mounted-volume:gogurt-macos-mounted-volume-discover-mount-points:336bb0c090 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-mounted-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0af3a7cf7d"></a>
- <a id="s-616e64bdef"></a>`distribution`: `gogurt-macos-mounted-volume`
- <a id="s-be4d871d83"></a>`module`: `gogurt_macos_mounted_volume`
- <a id="s-3cce1d9f83"></a>`name`: `discover_mount_points`
- <a id="s-2ad1c1d1e6"></a>`unit`: `export`

### Declared structure

- <a id="s-e5b358618c"></a>`kind`: `"function"`
- <a id="s-f589c19c81"></a>`signature`: `"\"() -> 'tuple[Path, ...]'\""`

## Governing policies

- <a id="pa-3286588246"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-macos-mounted-volume:gogurt_macos_mounted_volume](../../../evidence/sources.md#src-d5c41c5cfc) — [reference/gogurt/mounted-volume/macos/src/gogurt\_macos\_mounted\_volume/\_\_init\_\_.py](../../../../../../reference/gogurt/mounted-volume/macos/src/gogurt_macos_mounted_volume/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_macos_mounted_volume.discover_mount_points`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 92f60801c68abd3540c0ba64f90f4b737534239b04adec8e93e8b66864785d98 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'tuple[Path, ...]'\""
  },
  "distribution": "gogurt-macos-mounted-volume",
  "module": "gogurt_macos_mounted_volume",
  "name": "discover_mount_points",
  "unit": "export"
}
```

</details>
