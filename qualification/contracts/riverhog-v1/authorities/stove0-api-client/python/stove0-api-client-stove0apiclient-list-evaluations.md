# stove0_api_client.Stove0ApiClient.list_evaluations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-list-evaluations:092c2f6937 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f4ac9535ab"></a>
- <a id="s-cba58397d0"></a>`distribution`: `stove0-api-client`
- <a id="s-b903e8a746"></a>`module`: `stove0_api_client`
- <a id="s-74e325063f"></a>`name`: `list_evaluations`
- <a id="s-970b46300f"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-b4e0ab3938"></a>`unit`: `member`

### Declared structure

- <a id="s-a83b5360b1"></a>`kind`: `"method"`
- <a id="s-43ee3ed7b2"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None, phase: 'EvaluationPhase \| None' = None, query: 'str \| None' = None, sort: 'EvaluationSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'EvaluationPage'\""`

## Maintained corroboration

### Related interface records

- [stove0 evaluation list](../../stove0-client/cli/stove0-evaluation-list.md)
- [GET /v1/evaluations](../../stove0/http-operations/get-v1-evaluations.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-96f0e9a0d8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — [reference/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [reference/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.list\_evaluations](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L347)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.list_evaluations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f4c3eb227733ebfe7be3838bf5562288bc3e5cfa0229f818e1c6265cee1f6623 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, phase: 'EvaluationPhase | None' = None, query: 'str | None' = None, sort: 'EvaluationSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'EvaluationPage'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "list_evaluations",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
