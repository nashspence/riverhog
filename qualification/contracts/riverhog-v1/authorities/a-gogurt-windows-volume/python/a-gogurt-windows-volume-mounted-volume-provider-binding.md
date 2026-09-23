# a_gogurt_windows_volume.MOUNTED_VOLUME_PROVIDER_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-volume:a-gogurt-windows-volume-mounted-volume-pr-e9fc449171:3423be72e0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c9fb9d0b11"></a>
- <a id="s-997242a429"></a>`distribution`: `a-gogurt-windows-volume`
- <a id="s-82f2f46328"></a>`module`: `a_gogurt_windows_volume`
- <a id="s-ba69a4f493"></a>`name`: `MOUNTED_VOLUME_PROVIDER_BINDING`
- <a id="s-36e4ae934b"></a>`unit`: `export`

### Declared structure

- <a id="s-aa82cb0f29"></a>`kind`: `"object"`
- <a id="s-a82a77a3f2"></a>`type`: `"gogurt_core.mounts.MountedVolumeProviderBinding"`

## Governing policies

- <a id="pa-689988f537"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-volume:a_gogurt_windows_volume](../../../evidence/sources/authorities.md#src-4e8d7bf8db) — [some-implementations/gogurt/mounted-volume/windows/src/a\_gogurt\_windows\_volume/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/windows/src/a_gogurt_windows_volume/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_volume.MOUNTED_VOLUME_PROVIDER_BINDING`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9a2c276bda636c4ddc35a1ab59e013e548017675765f2ad5a4c7f1541da82626 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "gogurt_core.mounts.MountedVolumeProviderBinding"
  },
  "distribution": "a-gogurt-windows-volume",
  "module": "a_gogurt_windows_volume",
  "name": "MOUNTED_VOLUME_PROVIDER_BINDING",
  "unit": "export"
}
```

</details>
