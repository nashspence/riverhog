# stove0_api_client.Stove0ApiClient.cancel_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-cancel-evaluation:9c450a2de5 -->

Exact externally visible contract owned by this semantic dossier.

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

- [stove0_api_client.Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-3cd00ac90c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.cancel_evaluation`

### Exact owned JSON

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
