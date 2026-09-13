# stove0_api_client.Stove0ApiClient.step_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-step-evaluation:3c53021be3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eea4ad0532"></a>
| Field | Shape |
|---|---|
| <a id="s-deb9085dc0"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a53e46a121"></a>`distribution` | "stove0-api-client" |
| <a id="s-fa15686d20"></a>`module` | "stove0_api_client" |
| <a id="s-12a4dc3b8c"></a>`name` | "step_evaluation" |
| <a id="s-6739c2363c"></a>`owner` | "stove0_api_client.Stove0ApiClient" |
| <a id="s-ca0181a394"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_api_client.Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-16b9589f5e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.step_evaluation`

### Exact owned JSON

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
