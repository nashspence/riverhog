# stove0_review_sampler_support.SAMPLER_CONFORMANCE_RESULT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-sampler-con-44ded22e1a:979eeb441c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fdf519dd1a"></a>
| Field | Shape |
|---|---|
| <a id="s-0741b1cfe9"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-b3e9fce187"></a>`distribution` | "stove0-review-sampler-support" |
| <a id="s-7c3097685e"></a>`module` | "stove0_review_sampler_support" |
| <a id="s-ff51c03f34"></a>`name` | "SAMPLER_CONFORMANCE_RESULT" |
| <a id="s-1bb4f0f9ce"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c1a9ed21fe"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.SAMPLER_CONFORMANCE_RESULT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 614398eefd8622fe18c8159ff059d1b27afa4aa20133e3b9c3e003f19a0b1524 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-review-sampler-conformance-result/v1"
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "SAMPLER_CONFORMANCE_RESULT",
  "unit": "export"
}
```
