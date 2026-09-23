# a_gogurt_windows_volume.discover_mount_points

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-volume:a-gogurt-windows-volume-discover-mount-points:9b623ae7a7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f9a9a9721f"></a>
- <a id="s-dd801fe320"></a>`distribution`: `a-gogurt-windows-volume`
- <a id="s-cc05f70508"></a>`module`: `a_gogurt_windows_volume`
- <a id="s-f11109c331"></a>`name`: `discover_mount_points`
- <a id="s-f082d7ea2a"></a>`unit`: `export`

### Declared structure

- <a id="s-49fda2cc3b"></a>`kind`: `"function"`
- <a id="s-ec47b7f44b"></a>`signature`: `"\"() -> 'tuple[Path, ...]'\""`

## Governing policies

- <a id="pa-bc2b79eb4a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-volume:a_gogurt_windows_volume](../../../evidence/sources/authorities.md#src-4e8d7bf8db) — [some-implementations/gogurt/mounted-volume/windows/src/a\_gogurt\_windows\_volume/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/windows/src/a_gogurt_windows_volume/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_volume.discover_mount_points`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e56d91880b255ddf80c23349910e190641859b0a3ea6a1f84ee30f6b3977e9ad -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'tuple[Path, ...]'\""
  },
  "distribution": "a-gogurt-windows-volume",
  "module": "a_gogurt_windows_volume",
  "name": "discover_mount_points",
  "unit": "export"
}
```

</details>
