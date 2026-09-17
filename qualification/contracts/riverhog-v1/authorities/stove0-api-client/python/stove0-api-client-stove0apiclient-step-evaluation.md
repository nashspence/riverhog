# stove0_api_client.Stove0ApiClient.step_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-step-evaluation:3c53021be3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eea4ad0532"></a>
- <a id="s-a53e46a121"></a>`distribution`: `stove0-api-client`
- <a id="s-fa15686d20"></a>`module`: `stove0_api_client`
- <a id="s-12a4dc3b8c"></a>`name`: `step_evaluation`
- <a id="s-6739c2363c"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-ca0181a394"></a>`unit`: `member`

### Declared structure

- <a id="s-f639aa0e1d"></a>`kind`: `"method"`
- <a id="s-299f2121b6"></a>`signature`: `"\"(self, evaluation_id: 'str') -> 'EvaluationView'\""`

## Maintained corroboration

### Related interface records

- [stove0 evaluation step](../../stove0-client/cli/stove0-evaluation-step.md)
- [POST /v1/evaluations/{evaluation_id}/step](../../stove0/http-operations/post-v1-evaluations-evaluation-id-step.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-16b9589f5e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [reference/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [reference/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.step\_evaluation](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L403)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.step_evaluation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 571458c95374ac38439409755e76c625fb551350bca1ae9b09399d60daab0b08 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str') -> 'EvaluationView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "step_evaluation",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
