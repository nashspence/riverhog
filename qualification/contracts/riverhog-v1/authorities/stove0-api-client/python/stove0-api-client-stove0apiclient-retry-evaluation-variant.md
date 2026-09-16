# stove0_api_client.Stove0ApiClient.retry_evaluation_variant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-retry-e-c6006ad812:3b41be4858 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d0c2ade836"></a>
- <a id="s-9247891fb1"></a>`distribution`: `stove0-api-client`
- <a id="s-05584b74ab"></a>`module`: `stove0_api_client`
- <a id="s-b609e500f9"></a>`name`: `retry_evaluation_variant`
- <a id="s-b675a5cf61"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-e7a4919c81"></a>`unit`: `member`

### Declared structure

- <a id="s-1caacb3924"></a>`kind`: `"method"`
- <a id="s-fbf033ca27"></a>`signature`: `"\"(self, evaluation_id: 'str', variant_id: 'str') -> 'EvaluationView'\""`

## Maintained corroboration

### Related interface records

- [stove0 evaluation retry](../../stove0-client/cli/stove0-evaluation-retry.md)
- [POST /v1/evaluations/{evaluation_id}/variants/{variant_id}/retry](../../stove0/http-operations/post-v1-evaluations-evaluation-id-variants-variant-id-retry.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-f43612b334"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`
- **Client method:** [reference/stove0/packages/api-client/src/stove0_api_client/client.py::Stove0ApiClient.retry_evaluation_variant](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L419)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.retry_evaluation_variant`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 028bfdb6623c80b3ef42b2316bb759301ec4db66cc7d00ad8ddc66647c391103 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str', variant_id: 'str') -> 'EvaluationView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "retry_evaluation_variant",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
