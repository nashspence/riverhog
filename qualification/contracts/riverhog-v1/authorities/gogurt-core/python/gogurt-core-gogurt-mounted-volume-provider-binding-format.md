# gogurt_core.GOGURT_MOUNTED_VOLUME_PROVIDER_BINDING_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurt-mounted-volume-provide-df5e146872:1a380672fe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1d013336e4"></a>
| Field | Shape |
|---|---|
| <a id="s-07e6ffd8b2"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-2c3dbf39cd"></a>`distribution` | "gogurt-core" |
| <a id="s-9bb823edd8"></a>`module` | "gogurt_core" |
| <a id="s-2be77ac710"></a>`name` | "GOGURT_MOUNTED_VOLUME_PROVIDER_BINDING_FORMAT" |
| <a id="s-478273b381"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1352ec2452"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.GOGURT_MOUNTED_VOLUME_PROVIDER_BINDING_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7e61bfe95a2daf79ac7ae4cb0153bc3691c6bb2aeeaeaf0a2f08bdfee701438 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt-mounted-volume-provider-binding/v1"
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "GOGURT_MOUNTED_VOLUME_PROVIDER_BINDING_FORMAT",
  "unit": "export"
}
```
