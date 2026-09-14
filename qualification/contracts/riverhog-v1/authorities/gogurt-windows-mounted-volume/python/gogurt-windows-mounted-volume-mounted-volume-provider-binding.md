# gogurt_windows_mounted_volume.MOUNTED_VOLUME_PROVIDER_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-windows-mounted-volume:gogurt-windows-mounted-volume-mounted-vol-a3fcf0223c:b0146508b8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-mounted-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ce11da37a4"></a>
- <a id="s-ea3cfd2c6f"></a>`distribution`: `gogurt-windows-mounted-volume`
- <a id="s-9c0a980996"></a>`module`: `gogurt_windows_mounted_volume`
- <a id="s-3334dfb1c7"></a>`name`: `MOUNTED_VOLUME_PROVIDER_BINDING`
- <a id="s-ac103641a2"></a>`unit`: `export`

### Declared structure

- <a id="s-55b005a5bc"></a>`kind`: `"object"`
- <a id="s-2a66537c78"></a>`type`: `"gogurt_core.mounts.MountedVolumeProviderBinding"`

## Governing policies

- <a id="pa-e73e8c7528"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-windows-mounted-volume:gogurt_windows_mounted_volume](../../../evidence/sources.md#src-3af4750524) — `reference/gogurt/mounted-volume/windows/src/gogurt_windows_mounted_volume/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_windows_mounted_volume.MOUNTED_VOLUME_PROVIDER_BINDING`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd9d04092cf8ca615bc4133a4a14eba2a24e7898572d25db0a2cd2319626a489 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "gogurt_core.mounts.MountedVolumeProviderBinding"
  },
  "distribution": "gogurt-windows-mounted-volume",
  "module": "gogurt_windows_mounted_volume",
  "name": "MOUNTED_VOLUME_PROVIDER_BINDING",
  "unit": "export"
}
```
