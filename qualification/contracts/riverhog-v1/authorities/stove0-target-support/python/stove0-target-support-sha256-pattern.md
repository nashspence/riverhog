# stove0_target_support.SHA256_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-sha256-pattern:238da58622 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-39c8e256d5"></a>
| Field | Shape |
|---|---|
| <a id="s-35f05aa178"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-bdcfd1c37a"></a>`distribution` | "stove0-target-support" |
| <a id="s-8ee6839bd8"></a>`module` | "stove0_target_support" |
| <a id="s-23a9754611"></a>`name` | "SHA256_PATTERN" |
| <a id="s-9b417ae057"></a>`unit` | "export" |

## Governing policies

- <a id="pa-717786e82d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.SHA256_PATTERN`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5607316ff22a5df59d6118b85cdd7bb7f61dcc6d5d24ce2122dcd368507ed2f2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^[0-9a-f]{64}$"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "SHA256_PATTERN",
  "unit": "export"
}
```
