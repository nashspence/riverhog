# gogurt_macos_mounted_volume.MOUNTED_VOLUME_PROVIDER_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-mounted-volume:gogurt-macos-mounted-volume-mounted-volum-7fbcc0ce39:597e2d67cc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-mounted-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-96d6c492f6"></a>
- <a id="s-b7634719d7"></a>`distribution`: `gogurt-macos-mounted-volume`
- <a id="s-884a7b5ffa"></a>`module`: `gogurt_macos_mounted_volume`
- <a id="s-0507dd4792"></a>`name`: `MOUNTED_VOLUME_PROVIDER_BINDING`
- <a id="s-56fc5689d8"></a>`unit`: `export`

### Declared structure

- <a id="s-2103e018f9"></a>`kind`: `"object"`
- <a id="s-2c31521fa7"></a>`type`: `"gogurt_core.mounts.MountedVolumeProviderBinding"`

## Governing policies

- <a id="pa-50a7fdd18d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-macos-mounted-volume:gogurt_macos_mounted_volume](../../../evidence/sources.md#src-d5c41c5cfc) — `reference/gogurt/mounted-volume/macos/src/gogurt_macos_mounted_volume/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_macos_mounted_volume.MOUNTED_VOLUME_PROVIDER_BINDING`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aaa311ea19fe1146a694c346884e4eb850a0866a1d664ba37d0cb4077f0c65e5 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "gogurt_core.mounts.MountedVolumeProviderBinding"
  },
  "distribution": "gogurt-macos-mounted-volume",
  "module": "gogurt_macos_mounted_volume",
  "name": "MOUNTED_VOLUME_PROVIDER_BINDING",
  "unit": "export"
}
```

</details>
