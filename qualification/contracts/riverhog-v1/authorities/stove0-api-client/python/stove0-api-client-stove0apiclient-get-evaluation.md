# stove0_api_client.Stove0ApiClient.get_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-get-evaluation:8742160236 -->

Exact externally visible contract owned by this contract element.

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

- [stove0 evaluation show](../../stove0-client/cli/stove0-evaluation-show.md)
- [GET /v1/evaluations/{evaluation_id}](../../stove0/http-operations/get-v1-evaluations-evaluation-id.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-798b2e57df"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [reference/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [reference/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.get\_evaluation](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L398)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.get_evaluation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
