# Operation parity: cancel_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-cancel-evaluation:34c2bde7b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [evaluations](families/evaluations/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-afdcc1faa890"></a>
| Concern | Contract |
|---|---|
| <a id="s-f5a197a638a5"></a>`application` | stove0 |
| <a id="s-19b15f5c5ffe"></a>`classification` | human-cli+json |
| <a id="s-c29292045f9e"></a>`cli_commands` | ["evaluation cancel"] |
| <a id="s-1f51c66e442c"></a>`client` | Stove0ApiClient |
| <a id="s-3fbd053f9b6a"></a>`method` | POST |
| <a id="s-e06fbe880875"></a>`operation_id` | cancel_evaluation |
| <a id="s-36b802875706"></a>`path` | /v1/evaluations/{evaluation_id}/cancel |
| <a id="s-19131c6adfbb"></a>`provider_evidence` | None |
| <a id="s-dde4f214486a"></a>`read_collection` | None |
| <a id="s-a110bba55d70"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations/{evaluation_id}/cancel](../http/post-v1-evaluations-evaluation-id-cancel.md)
- [stove0 evaluation cancel](../cli/stove0-evaluation-cancel.md)

## Governing policies

- <a id="pa-dd57f819e6c7"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-7e56a8df276f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-d3fbf9f70065"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/127`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c947b5a6f051a09bcb17da65b09eed2d1d4620b91c98f8feb9000fbb9430b744 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "evaluation cancel"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "cancel_evaluation",
  "path": "/v1/evaluations/{evaluation_id}/cancel",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
