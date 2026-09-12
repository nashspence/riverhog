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

<a id="s-159f17d5b2"></a>
| Concern | Contract |
|---|---|
| <a id="s-c63d25d86a"></a>`application` | stove0 |
| <a id="s-d6657b0b92"></a>`classification` | human-cli+json |
| <a id="s-b6328da0de"></a>`cli_commands` | ["evaluation review"] |
| <a id="s-5a827b7fe6"></a>`client` | Stove0ApiClient |
| <a id="s-3496d9fefb"></a>`method` | PUT |
| <a id="s-aa6c42fc2b"></a>`operation_id` | review_evaluation_variant |
| <a id="s-f2e2496a86"></a>`path` | /v1/evaluations/{evaluation_id}/variants/{variant_id}/review |
| <a id="s-4c545e5b19"></a>`provider_evidence` | None |
| <a id="s-5b80b20ad6"></a>`read_collection` | None |
| <a id="s-03bdf3ccf8"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [PUT /v1/evaluations/{evaluation_id}/variants/{variant_id}/review](../http/put-v1-evaluations-evaluation-id-variants-variant-id-review.md)
- [stove0-client evaluation review](../../stove0-client/cli/stove0-client-evaluation-review.md)

## Governing policies

- <a id="pa-03c9243b5f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-2b1017465a"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-f577376219"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

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
