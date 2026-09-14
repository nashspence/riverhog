# gogurt_core.MountedVolumeProviderBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-mountedvolumeproviderbinding:13919d3ec2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3fecbe809e"></a>
- <a id="s-06717b2ee9"></a>`distribution`: `gogurt-core`
- <a id="s-89ab919573"></a>`module`: `gogurt_core`
- <a id="s-58ca96509d"></a>`name`: `MountedVolumeProviderBinding`
- <a id="s-c6f14e34a3"></a>`unit`: `export`

### Declared structure

- <a id="s-5a5deba83b"></a>`kind`: `"class"`
- <a id="s-08ea9d94c6"></a>`signature`: `"\"(provider_id: 'str', access: 'MountedVolumeAccess', format: 'str' = 'gogurt-mounted-volume-provider-binding/v1') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-30ba0047fe"></a>`provider_id` | `'str'` | `required` |
| <a id="s-bb815156e3"></a>`access` | `'MountedVolumeAccess'` | `required` |
| <a id="s-bdd423cb8a"></a>`format` | `'str'` | `'gogurt-mounted-volume-provider-binding/v1'` |

## Governing policies

- <a id="pa-74bf6f184b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.MountedVolumeProviderBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f98c042e241a76a23a3d5ea9a372aedc3ed9afc3845a0d433cf25f58cb2493e7 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "provider_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "access",
        "type": "'MountedVolumeAccess'"
      },
      {
        "default": "'gogurt-mounted-volume-provider-binding/v1'",
        "name": "format",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(provider_id: 'str', access: 'MountedVolumeAccess', format: 'str' = 'gogurt-mounted-volume-provider-binding/v1') -> None\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "MountedVolumeProviderBinding",
  "unit": "export"
}
```
