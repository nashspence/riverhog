# stove0_target_support.TargetProtocolModel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetprotocolmodel:f650909d59 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-deccbcd5af"></a>
| Field | Shape |
|---|---|
| <a id="s-337b72dc3c"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-5ca1635ebf"></a>`distribution` | "stove0-target-support" |
| <a id="s-b730c98ed4"></a>`module` | "stove0_target_support" |
| <a id="s-bf4669648d"></a>`name` | "TargetProtocolModel" |
| <a id="s-c0ac5d6432"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f4c5ce0bb1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetProtocolModel`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6019ec96034801d13e2a5a96cb963b0bf54554caeed3e7280a7cddc025c4222 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c93552e6289487dd5fa8baeb3416b24ed8657c354167fb0ed7a8de1e1a8ccb90",
    "signature": "'() -> None'"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetProtocolModel",
  "unit": "export"
}
```
