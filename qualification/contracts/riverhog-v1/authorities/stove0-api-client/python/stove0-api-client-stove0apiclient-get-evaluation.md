# stove0_api_client.Stove0ApiClient.get_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-get-evaluation:8742160236 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-13838f7157"></a>
- <a id="s-6f5093e849"></a>`distribution`: `stove0-api-client`
- <a id="s-3c805da46c"></a>`module`: `stove0_api_client`
- <a id="s-1865982ce4"></a>`name`: `get_evaluation`
- <a id="s-8a7f03948d"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-d343185bf8"></a>`unit`: `member`

### Declared structure

- <a id="s-5de08bf93e"></a>`kind`: `"method"`
- <a id="s-56d417dc4b"></a>`signature`: `"\"(self, evaluation_id: 'str') -> 'EvaluationView'\""`

## Maintained corroboration

### Related interface records

- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-798b2e57df"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.get_evaluation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb533952b9080dfdcba22ec4494e91ad7cb00554275d8a51f0b9bd83bbb8a658 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str') -> 'EvaluationView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "get_evaluation",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```
