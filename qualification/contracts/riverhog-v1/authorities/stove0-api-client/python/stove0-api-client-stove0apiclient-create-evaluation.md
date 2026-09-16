# stove0_api_client.Stove0ApiClient.create_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-create-evaluation:5aef1d8572 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4f3fbfe199"></a>
- <a id="s-4422654547"></a>`distribution`: `stove0-api-client`
- <a id="s-0b1df8c9ef"></a>`module`: `stove0_api_client`
- <a id="s-7cad4f728f"></a>`name`: `create_evaluation`
- <a id="s-e1ba765b67"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-ce8ad86ee8"></a>`unit`: `member`

### Declared structure

- <a id="s-4ade5b57c4"></a>`kind`: `"method"`
- <a id="s-582649594f"></a>`signature`: `"\"(self, definition: 'Mapping[str, Any]') -> 'EvaluationView'\""`

## Maintained corroboration

### Related interface records

- [stove0 evaluation create](../../stove0-client/cli/stove0-evaluation-create.md)
- [POST /v1/evaluations](../../stove0/http-operations/post-v1-evaluations.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-3d08a692fb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`
- **Client method:** [reference/stove0/packages/api-client/src/stove0_api_client/client.py::Stove0ApiClient.create_evaluation](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L387)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.create_evaluation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8fe72dc1a48d9901cef569268baa8e7c9ba8620ef6ac696a7e3503eb4e82f7a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, definition: 'Mapping[str, Any]') -> 'EvaluationView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "create_evaluation",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```
