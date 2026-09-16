# gogurt_linux_mounted_volume.MOUNTED_VOLUME_PROVIDER_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-mounted-volume:gogurt-linux-mounted-volume-mounted-volum-ae68b7a889:0baf25f1e6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-mounted-volume](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9f7e4dd604"></a>
- <a id="s-db108b66da"></a>`distribution`: `gogurt-linux-mounted-volume`
- <a id="s-a8ff496699"></a>`module`: `gogurt_linux_mounted_volume`
- <a id="s-ef03b7c76b"></a>`name`: `MOUNTED_VOLUME_PROVIDER_BINDING`
- <a id="s-604bdca6c5"></a>`unit`: `export`

### Declared structure

- <a id="s-b6accd570b"></a>`kind`: `"object"`
- <a id="s-6286eba6d3"></a>`type`: `"gogurt_core.mounts.MountedVolumeProviderBinding"`

## Governing policies

- <a id="pa-3c555bd80a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-linux-mounted-volume:gogurt_linux_mounted_volume](../../../evidence/sources.md#src-dfbc0b0c2f) — `reference/gogurt/mounted-volume/linux/src/gogurt_linux_mounted_volume/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_linux_mounted_volume.MOUNTED_VOLUME_PROVIDER_BINDING`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f9ba12f3932b4bcbf45654d4d67d54ed146e58c9e9ba5979f799805e8302fad4 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "gogurt_core.mounts.MountedVolumeProviderBinding"
  },
  "distribution": "gogurt-linux-mounted-volume",
  "module": "gogurt_linux_mounted_volume",
  "name": "MOUNTED_VOLUME_PROVIDER_BINDING",
  "unit": "export"
}
```

</details>
