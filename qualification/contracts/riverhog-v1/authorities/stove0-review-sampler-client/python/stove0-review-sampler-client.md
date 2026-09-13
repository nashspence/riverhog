# stove0_review_sampler_client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-client:stove0-review-sampler-client:e892fbfaf9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-client](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-a7636c09bb) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-59c08cf139"></a>
| Field | Shape |
|---|---|
| <a id="s-450acacb69"></a>`candidate_id` | "python:stove0-review-sampler-client:stove0_review_sampler_client" |
| <a id="s-2167bd7974"></a>`distribution` | "stove0-review-sampler-client" |
| <a id="s-3a118d3923"></a>`exports` | additional keys=`ReviewSamplerClient`, `SamplerProtocolError` |
| <a id="s-2d3b12f4c0"></a>`module` | "stove0_review_sampler_client" |

## Governing policies

- <a id="pa-936e10c64f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-client:stove0_review_sampler_client](../../../evidence/sources.md#src-4a777c675f) — `reference/stove0/targets/review/sampler/client/src/stove0_review_sampler_client/__init__.py`

### Machine authority

- `/external_contract/python/52`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 87b5594a4df4c29bc235878f8865535d33d9c3a2e536b4ae2f64fa946db83026 -->

```json
{
  "candidate_id": "python:stove0-review-sampler-client:stove0_review_sampler_client",
  "distribution": "stove0-review-sampler-client",
  "exports": {
    "ReviewSamplerClient": {
      "kind": "class",
      "members": {
        "close": {
          "kind": "method",
          "signature": "\"(self) -> 'None'\""
        },
        "descriptor": {
          "kind": "method",
          "signature": "\"(self, *, refresh: 'bool' = False) -> 'SamplerDescriptor'\""
        },
        "sample": {
          "kind": "method",
          "signature": "\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""
        }
      },
      "signature": "\"(base_url: 'str', token: 'str', *, allow_insecure_http: 'bool' = False, timeout_seconds: 'float' = 86400, transport: 'httpx.BaseTransport | None' = None) -> 'None'\""
    },
    "SamplerProtocolError": {
      "kind": "class",
      "signature": "'(message: \\'str\\', *, failure_kind: \"Literal[\\'remote_rejection\\', \\'transport\\', \\'invalid_response\\']\", code: \\'str | None\\' = None, observed_status: \\'int | None\\' = None, details: \\'Mapping[str, Any] | None\\' = None) -> \\'None\\''"
    }
  },
  "module": "stove0_review_sampler_client"
}
```
