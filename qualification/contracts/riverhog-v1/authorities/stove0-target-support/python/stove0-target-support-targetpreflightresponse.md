# stove0_target_support.TargetPreflightResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetpreflightresponse:cb5681b311 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fd7781d208"></a>
| Field | Shape |
|---|---|
| <a id="s-ea4f5d8a6a"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-73a5d587ed"></a>`distribution` | "stove0-target-support" |
| <a id="s-3fb9905ddd"></a>`module` | "stove0_target_support" |
| <a id="s-cea0734902"></a>`name` | "TargetPreflightResponse" |
| <a id="s-7c62edc6c6"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetPreflightResponse.bind_protocol](stove0-target-support-targetpreflightresponse-bind-protocol.md)

## Governing policies

- <a id="pa-6d30fd4c40"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetPreflightResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d29ad9da414ca84c1ae6c05a0f76885ebaf1353ca3203d41ee95501373f79cf5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "2de154a75245f705341e8f659ce900ce6f3b2bef59a16b22d7980249391824e9",
    "signature": "'(*, target: stove0_target_protocol.protocol.TargetContract, plan: stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan) -> None'"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetPreflightResponse",
  "unit": "export"
}
```
