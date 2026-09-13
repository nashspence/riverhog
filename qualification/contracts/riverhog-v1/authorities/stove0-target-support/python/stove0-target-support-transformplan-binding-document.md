# stove0_target_support.TransformPlan.binding_document

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-transformplan-binding-document:df98b77223 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3e402bc376"></a>
| Field | Shape |
|---|---|
| <a id="s-5edbe4d8ff"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-21e42025c0"></a>`distribution` | "stove0-target-support" |
| <a id="s-4c57ca468a"></a>`module` | "stove0_target_support" |
| <a id="s-5e4f8b244b"></a>`name` | "binding_document" |
| <a id="s-29b93364a2"></a>`owner` | "stove0_target_support.TransformPlan" |
| <a id="s-33523a54c7"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TransformPlan](stove0-target-support-transformplan.md)

## Governing policies

- <a id="pa-78cd97c9c2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TransformPlan.binding_document`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da3e0c84a2d1907edd7c5f5ec7cd62bfdfa4ab1a63e72a26f1733ce3a421bc5f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, JsonValue]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "binding_document",
  "owner": "stove0_target_support.TransformPlan",
  "unit": "member"
}
```
