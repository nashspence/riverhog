# stove0_review_target_support.SamplerRegistration.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-samplerregis-14dadda07e:443e2d8835 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-41ca2be81b"></a>
- <a id="s-0ead72d46a"></a>`distribution`: `stove0-review-target-support`
- <a id="s-b46424e25b"></a>`module`: `stove0_review_target_support`
- <a id="s-e7cada9020"></a>`name`: `descriptor`
- <a id="s-34120ee5e9"></a>`owner`: `stove0_review_target_support.SamplerRegistration`
- <a id="s-0bd0d82b1e"></a>`unit`: `member`

### Declared structure

- <a id="s-4aaa0f158d"></a>`kind`: `"method"`
- <a id="s-391a937e8c"></a>`signature`: `"\"(self) -> 'SamplerDescriptor'\""`

## Maintained corroboration

### Related interface records

- [SamplerRegistration](stove0-review-target-support-samplerregistration.md)

## Governing policies

- <a id="pa-8e9ca1d56e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources/authorities.md#src-2a89a71c41) — [reference/stove0/targets/review/support/src/stove0\_review\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_support.SamplerRegistration.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea9077a6c0ea178dc7426a6fd09e20231f12ae1bd715054861368adbe6a3ff54 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'SamplerDescriptor'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "descriptor",
  "owner": "stove0_review_target_support.SamplerRegistration",
  "unit": "member"
}
```

</details>
