# stove0_target_support.TargetHttpBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targethttpbinding:12e4dbcfb2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-54295f660d"></a>
| Field | Shape |
|---|---|
| <a id="s-9eec3bb9e1"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-6d35e5cb9f"></a>`distribution` | "stove0-target-support" |
| <a id="s-6b2fce611d"></a>`module` | "stove0_target_support" |
| <a id="s-8a365315b6"></a>`name` | "TargetHttpBinding" |
| <a id="s-f36170af40"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetHttpBinding.handle](stove0-target-support-targethttpbinding-handle.md)

## Governing policies

- <a id="pa-056cc4de55"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetHttpBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e7f694b0f945270dc477fcc90825c71c9e07fb7c6868439f64fdee0ee682fc7 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(target: 'TargetService', *, maximum_request_bytes: 'int' = 16777216) -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetHttpBinding",
  "unit": "export"
}
```
