# stove0_review_target_support.parse_sampler_registrations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-parse-sample-8719acadf6:1f8bd92da0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bb7b01c888"></a>
- <a id="s-74d580e1c5"></a>`distribution`: `stove0-review-target-support`
- <a id="s-8bb7b5ab09"></a>`module`: `stove0_review_target_support`
- <a id="s-eab6d79abb"></a>`name`: `parse_sampler_registrations`
- <a id="s-743d4e8abc"></a>`unit`: `export`

### Declared structure

- <a id="s-fa51007872"></a>`kind`: `"function"`
- <a id="s-420ffe4913"></a>`signature`: `"\"(document: 'str') -> 'tuple[SamplerRegistration, ...]'\""`

## Governing policies

- <a id="pa-ba08f3f50c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.parse_sampler_registrations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a02def71d84cc189afe1206d72d8dcc43407710353cd0be0aaccc31950b03765 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(document: 'str') -> 'tuple[SamplerRegistration, ...]'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "parse_sampler_registrations",
  "unit": "export"
}
```

</details>
