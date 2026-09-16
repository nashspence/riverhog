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
- <a id="s-de11072a42"></a>`distribution`: `stove0-review-target-support`
- <a id="s-9e97ab2f84"></a>`module`: `stove0_review_target_support`
- <a id="s-ec8654615e"></a>`name`: `SamplerRegistration`
- <a id="s-517500f8be"></a>`unit`: `export`

### Declared structure

- <a id="s-e7509d7e60"></a>`kind`: `"class"`
- <a id="s-332a1c38fb"></a>`signature`: `"\"(id: 'str', client: 'ReviewSamplerClient', descriptor_sha256: 'str', image_digest: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-c6fe35252b"></a>`id` | `'str'` | `required` |
| <a id="s-b3547b4c35"></a>`client` | `'ReviewSamplerClient'` | `required` |
| <a id="s-d5b8dc9d13"></a>`descriptor_sha256` | `'str'` | `required` |
| <a id="s-05160ecc62"></a>`image_digest` | `'str'` | `required` |

## Maintained corroboration

### Related interface records

- [descriptor](stove0-review-target-support-samplerregistration-descriptor.md)

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

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
