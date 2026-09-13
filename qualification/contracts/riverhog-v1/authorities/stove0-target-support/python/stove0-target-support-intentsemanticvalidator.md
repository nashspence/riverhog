# stove0_target_support.IntentSemanticValidator

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-intentsemanticvalidator:4214fbb0c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f17180f6d8"></a>
| Field | Shape |
|---|---|
| <a id="s-2f960e72c7"></a>`contract` | type="collections.abc._CallableGenericAlias"; additional keys=`kind` |
| <a id="s-550f0cb40a"></a>`distribution` | "stove0-target-support" |
| <a id="s-0ea0c4b3c9"></a>`module` | "stove0_target_support" |
| <a id="s-5060928b6d"></a>`name` | "IntentSemanticValidator" |
| <a id="s-8d3ad0a4f8"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c82a279bed"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.IntentSemanticValidator`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b2064feb17cdfef8e0d406a05f441fd118074f2ec15888e2a815dc75c9b96842 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "collections.abc._CallableGenericAlias"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "IntentSemanticValidator",
  "unit": "export"
}
```
