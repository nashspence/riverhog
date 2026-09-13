# stove0_target_support.TransformPlanPayload.canonical_observation_results

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-transformplanpayloa-0c5bb78032:2aec790f24 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d9668d229f"></a>
| Field | Shape |
|---|---|
| <a id="s-81c1a447c9"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ca1fc7f5ba"></a>`distribution` | "stove0-target-support" |
| <a id="s-e95a866e46"></a>`module` | "stove0_target_support" |
| <a id="s-03e8db29c5"></a>`name` | "canonical_observation_results" |
| <a id="s-fe124abca0"></a>`owner` | "stove0_target_support.TransformPlanPayload" |
| <a id="s-e761cf075f"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TransformPlanPayload](stove0-target-support-transformplanpayload.md)

## Governing policies

- <a id="pa-9f1f91be3c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TransformPlanPayload.canonical_observation_results`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d78d1f24f3cad3f5fa871b2b1f65e8c0ee6c6f01adc97ab82120c0a5cfbad2f -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_observation_results",
  "owner": "stove0_target_support.TransformPlanPayload",
  "unit": "member"
}
```
