# a_gogurt_macos_volume.macos_mount_points

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-volume:a-gogurt-macos-volume-macos-mount-points:72ceaa5e5b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-00be4c4af8"></a>
- <a id="s-329429a55e"></a>`distribution`: `a-gogurt-macos-volume`
- <a id="s-987653e0c6"></a>`module`: `a_gogurt_macos_volume`
- <a id="s-425a0b6983"></a>`name`: `macos_mount_points`
- <a id="s-1ccf777b1a"></a>`unit`: `export`

### Declared structure

- <a id="s-1476d9f257"></a>`kind`: `"function"`
- <a id="s-44cff06af9"></a>`signature`: `"\"(volumes_dir: 'Path' = PosixPath('/Volumes')) -> 'tuple[Path, ...]'\""`

## Governing policies

- <a id="pa-beabc3bdb3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-volume:a_gogurt_macos_volume](../../../evidence/sources/authorities.md#src-febb07a039) — [some-implementations/gogurt/mounted-volume/macos/src/a\_gogurt\_macos\_volume/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/macos/src/a_gogurt_macos_volume/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_volume.macos_mount_points`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3008bf4edb86fa2356b2b719f21cdc5143c628356de3a8101182618b9c5982b1 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(volumes_dir: 'Path' = PosixPath('/Volumes')) -> 'tuple[Path, ...]'\""
  },
  "distribution": "a-gogurt-macos-volume",
  "module": "a_gogurt_macos_volume",
  "name": "macos_mount_points",
  "unit": "export"
}
```

</details>
