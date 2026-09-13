# stove0_target_support.validate_preflight_response_against_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-validate-preflight-a19f367a9d:076f231e5e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aa3f95cdf5"></a>
| Field | Shape |
|---|---|
| <a id="s-7d71ebf2f4"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-654dd83060"></a>`distribution` | "stove0-target-support" |
| <a id="s-5b9789292c"></a>`module` | "stove0_target_support" |
| <a id="s-7608327ec7"></a>`name` | "validate_preflight_response_against_request" |
| <a id="s-82a0673fe2"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a97cecc86d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.validate_preflight_response_against_request`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd9c47bbadc87e0a8656d4091385053e1d966745be82220296d71938a44442ed -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(response: 'TargetPreflightResponse', request: 'TargetPreflightRequest') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "validate_preflight_response_against_request",
  "unit": "export"
}
```
