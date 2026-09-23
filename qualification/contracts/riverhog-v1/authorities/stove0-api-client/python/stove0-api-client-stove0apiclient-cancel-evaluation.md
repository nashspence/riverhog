# stove0_api_client.Stove0ApiClient.cancel_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-cancel-evaluation:9c450a2de5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-20d8119e58"></a>
- <a id="s-8f80199363"></a>`distribution`: `stove0-api-client`
- <a id="s-8c204c30a1"></a>`module`: `stove0_api_client`
- <a id="s-aba85f569c"></a>`name`: `cancel_evaluation`
- <a id="s-98fab8ffa6"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-65f5c64b5d"></a>`unit`: `member`

### Declared structure

- <a id="s-f190c16a48"></a>`kind`: `"method"`
- <a id="s-7ca3e2b6cf"></a>`signature`: `"\"(self, evaluation_id: 'str') -> 'EvaluationView'\""`

## Maintained corroboration

### Related interface records

- [stove0 evaluation cancel](../../a-stove0-cli/cli/stove0-evaluation-cancel.md)
- [POST /v1/evaluations/{evaluation_id}/cancel](../../stove0/http-operations/post-v1-evaluations-evaluation-id-cancel.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-3cd00ac90c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.cancel\_evaluation](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py#L410)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.cancel_evaluation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 99e2becf6a8952e3af13f70f3ee2e8bb4e565f2a27d8d9041d5fbfb9199cf3d4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str') -> 'EvaluationView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "cancel_evaluation",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
