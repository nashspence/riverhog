# stove0_review_target_support.SamplerRegistration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-samplerregistration:14701c1598 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bd66b0be49"></a>
| Field | Shape |
|---|---|
| <a id="s-8ecf4ab48b"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-de11072a42"></a>`distribution` | "stove0-review-target-support" |
| <a id="s-9e97ab2f84"></a>`module` | "stove0_review_target_support" |
| <a id="s-ec8654615e"></a>`name` | "SamplerRegistration" |
| <a id="s-517500f8be"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_target_support.SamplerRegistration.descriptor](stove0-review-target-support-samplerregistration-descriptor.md)

## Governing policies

- <a id="pa-f8205e4f5a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.SamplerRegistration`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4772658250ada856daa1b3bf1fef11fcf96a7d351daf3fb26ad733c9e1ec671f -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "client",
        "type": "'ReviewSamplerClient'"
      },
      {
        "default": "required",
        "name": "descriptor_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "image_digest",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(id: 'str', client: 'ReviewSamplerClient', descriptor_sha256: 'str', image_digest: 'str') -> None\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "SamplerRegistration",
  "unit": "export"
}
```
