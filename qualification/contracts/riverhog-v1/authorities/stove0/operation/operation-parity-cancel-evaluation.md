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

<a id="s-afdcc1faa8"></a>
| Concern | Contract |
|---|---|
| <a id="s-f5a197a638"></a>`application` | stove0 |
| <a id="s-19b15f5c5f"></a>`classification` | human-cli+json |
| <a id="s-c29292045f"></a>`cli_commands` | ["evaluation cancel"] |
| <a id="s-1f51c66e44"></a>`client` | Stove0ApiClient |
| <a id="s-3fbd053f9b"></a>`method` | POST |
| <a id="s-e06fbe8808"></a>`operation_id` | cancel_evaluation |
| <a id="s-36b8028757"></a>`path` | /v1/evaluations/{evaluation_id}/cancel |
| <a id="s-19131c6adf"></a>`provider_evidence` | None |
| <a id="s-dde4f21448"></a>`read_collection` | None |
| <a id="s-a110bba55d"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations/{evaluation_id}/cancel](../http/post-v1-evaluations-evaluation-id-cancel.md)
- [stove0 evaluation cancel](../cli/stove0-evaluation-cancel.md)

## Governing policies

- <a id="pa-dd57f819e6"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-7e56a8df27"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-d3fbf9f700"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

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
