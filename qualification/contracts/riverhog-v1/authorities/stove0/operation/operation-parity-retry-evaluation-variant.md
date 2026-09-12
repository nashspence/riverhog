# Operation parity: retry_evaluation_variant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-retry-evaluation-variant:17f3defd1b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [evaluations](families/evaluations/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-211b3fd345"></a>
| Concern | Contract |
|---|---|
| <a id="s-3032bab166"></a>`application` | stove0 |
| <a id="s-ecc4b022ed"></a>`classification` | human-cli+json |
| <a id="s-56db84ab43"></a>`cli_commands` | ["evaluation retry"] |
| <a id="s-eaee9a5e11"></a>`client` | Stove0ApiClient |
| <a id="s-ae75d83ecf"></a>`method` | POST |
| <a id="s-46d3a47471"></a>`operation_id` | retry_evaluation_variant |
| <a id="s-f40eed7726"></a>`path` | /v1/evaluations/{evaluation_id}/variants/{variant_id}/retry |
| <a id="s-1195b10212"></a>`provider_evidence` | None |
| <a id="s-0a7a7f25a0"></a>`read_collection` | None |
| <a id="s-cc6de5369f"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations/{evaluation_id}/variants/{variant_id}/retry](../http/post-v1-evaluations-evaluation-id-variants-variant-id-retry.md)
- [stove0 evaluation retry](../cli/stove0-evaluation-retry.md)

## Governing policies

- <a id="pa-e9e812b927"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-f64f397e02"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-b3fa5701a2"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/129`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7613e8088d67c3277d778725d7d1672ecc5d58cd16982ea43ea68b5abb3a85c9 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "evaluation retry"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "retry_evaluation_variant",
  "path": "/v1/evaluations/{evaluation_id}/variants/{variant_id}/retry",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
