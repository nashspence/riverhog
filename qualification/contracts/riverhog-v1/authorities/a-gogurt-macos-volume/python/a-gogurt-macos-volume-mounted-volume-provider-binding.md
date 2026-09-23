# a_gogurt_macos_volume.MOUNTED_VOLUME_PROVIDER_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-volume:a-gogurt-macos-volume-mounted-volume-prov-260053f761:b9baa3d52c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-06ecfc49a5"></a>
- <a id="s-7b4f934dc0"></a>`distribution`: `a-gogurt-macos-volume`
- <a id="s-998a2b8b6b"></a>`module`: `a_gogurt_macos_volume`
- <a id="s-674c045e30"></a>`name`: `MOUNTED_VOLUME_PROVIDER_BINDING`
- <a id="s-ee984e4e14"></a>`unit`: `export`

### Declared structure

- <a id="s-6703adf3dd"></a>`kind`: `"object"`
- <a id="s-5019a11d40"></a>`type`: `"gogurt_core.mounts.MountedVolumeProviderBinding"`

## Governing policies

- <a id="pa-3bc39b9517"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-volume:a_gogurt_macos_volume](../../../evidence/sources/authorities.md#src-febb07a039) — [some-implementations/gogurt/mounted-volume/macos/src/a\_gogurt\_macos\_volume/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/macos/src/a_gogurt_macos_volume/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_volume.MOUNTED_VOLUME_PROVIDER_BINDING`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8318e9668d9f6929902d0c22742dc7e04bd3c58431d96e325454aecbce071263 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "gogurt_core.mounts.MountedVolumeProviderBinding"
  },
  "distribution": "a-gogurt-macos-volume",
  "module": "a_gogurt_macos_volume",
  "name": "MOUNTED_VOLUME_PROVIDER_BINDING",
  "unit": "export"
}
```

</details>
