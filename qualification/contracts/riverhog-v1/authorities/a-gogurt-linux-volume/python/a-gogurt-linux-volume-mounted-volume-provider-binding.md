# a_gogurt_linux_volume.MOUNTED_VOLUME_PROVIDER_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-volume:a-gogurt-linux-volume-mounted-volume-prov-7e482a6427:c534a4900a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a6c41a4677"></a>
- <a id="s-2fee3368a7"></a>`distribution`: `a-gogurt-linux-volume`
- <a id="s-783a1a6710"></a>`module`: `a_gogurt_linux_volume`
- <a id="s-4d66fe22fd"></a>`name`: `MOUNTED_VOLUME_PROVIDER_BINDING`
- <a id="s-199cff6446"></a>`unit`: `export`

### Declared structure

- <a id="s-d98b4264c5"></a>`kind`: `"object"`
- <a id="s-cc12582311"></a>`type`: `"gogurt_core.mounts.MountedVolumeProviderBinding"`

## Governing policies

- <a id="pa-68d5bff0f7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-volume:a_gogurt_linux_volume](../../../evidence/sources/authorities.md#src-7cf1a6536b) — [some-implementations/gogurt/mounted-volume/linux/src/a\_gogurt\_linux\_volume/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/linux/src/a_gogurt_linux_volume/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_volume.MOUNTED_VOLUME_PROVIDER_BINDING`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e27921ae8b2b275dabd8e4c72356550563c4ab8413eaa88c5c80df80aecba6c0 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "gogurt_core.mounts.MountedVolumeProviderBinding"
  },
  "distribution": "a-gogurt-linux-volume",
  "module": "a_gogurt_linux_volume",
  "name": "MOUNTED_VOLUME_PROVIDER_BINDING",
  "unit": "export"
}
```

</details>
