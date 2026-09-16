# stove0_api_client.Stove0ApiClient.review_evaluation_variant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-review-58c50b003b:ce6ee3bfb9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-77ec797383"></a>
- <a id="s-85994d2b69"></a>`distribution`: `stove0-api-client`
- <a id="s-f691b51ca6"></a>`module`: `stove0_api_client`
- <a id="s-6f42d21ca1"></a>`name`: `review_evaluation_variant`
- <a id="s-db64196f7e"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-0969fd5502"></a>`unit`: `member`

### Declared structure

- <a id="s-b8acb6277f"></a>`kind`: `"method"`
- <a id="s-cd857cf65b"></a>`signature`: `"\"(self, evaluation_id: 'str', variant_id: 'str', *, rating: 'int \| None' = None, note: 'str \| None' = None) -> 'EvaluationView'\""`

## Maintained corroboration

### Related interface records

- [stove0 evaluation review](../../stove0-client/cli/stove0-evaluation-review.md)
- [PUT /v1/evaluations/{evaluation_id}/variants/{variant_id}/review](../../stove0/http-operations/put-v1-evaluations-evaluation-id-variants-variant-id-review.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-a8f3e494da"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`
- **Client method:** [reference/stove0/packages/api-client/src/stove0_api_client/client.py::Stove0ApiClient.review_evaluation_variant](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L429)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.review_evaluation_variant`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 466e04d048087137321ddc0adafb9e457093e6faa5eddc6f9b13b406fc11d628 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str', variant_id: 'str', *, rating: 'int | None' = None, note: 'str | None' = None) -> 'EvaluationView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "review_evaluation_variant",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
