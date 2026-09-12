# Operation parity: review_evaluation_variant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-review-evaluation-variant:1a5900cfb5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [evaluations](families/evaluations/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-159f17d5b210"></a>
| Concern | Contract |
|---|---|
| <a id="s-c63d25d86a69"></a>`application` | stove0 |
| <a id="s-d6657b0b92c1"></a>`classification` | human-cli+json |
| <a id="s-b6328da0de56"></a>`cli_commands` | ["evaluation review"] |
| <a id="s-5a827b7fe6f9"></a>`client` | Stove0ApiClient |
| <a id="s-3496d9fefb86"></a>`method` | PUT |
| <a id="s-aa6c42fc2b6c"></a>`operation_id` | review_evaluation_variant |
| <a id="s-f2e2496a868c"></a>`path` | /v1/evaluations/{evaluation_id}/variants/{variant_id}/review |
| <a id="s-4c545e5b1906"></a>`provider_evidence` | None |
| <a id="s-5b80b20ad64c"></a>`read_collection` | None |
| <a id="s-03bdf3ccf832"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [PUT /v1/evaluations/{evaluation_id}/variants/{variant_id}/review](../http/put-v1-evaluations-evaluation-id-variants-variant-id-review.md)
- [stove0 evaluation review](../cli/stove0-evaluation-review.md)

## Governing policies

- <a id="pa-03c9243b5ff6"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-2b1017465a60"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-f5773762196f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/130`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bbf7a2af7e628242a794efe06f244899ed65da54ff8dc1707314d500e8eecd6c -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "evaluation review"
  ],
  "client": "Stove0ApiClient",
  "method": "PUT",
  "operation_id": "review_evaluation_variant",
  "path": "/v1/evaluations/{evaluation_id}/variants/{variant_id}/review",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
