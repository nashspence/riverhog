# Operation parity: step_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-step-evaluation:5c472d344c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [evaluations](families/evaluations/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-27b7249254"></a>
| Concern | Contract |
|---|---|
| <a id="s-c024337f6e"></a>`application` | stove0 |
| <a id="s-c47db2c7ea"></a>`classification` | human-cli+json |
| <a id="s-ee0060c6e4"></a>`cli_commands` | ["evaluation step"] |
| <a id="s-57f7c91eaa"></a>`client` | Stove0ApiClient |
| <a id="s-3c2a761651"></a>`method` | POST |
| <a id="s-dc7025bb38"></a>`operation_id` | step_evaluation |
| <a id="s-1181958452"></a>`path` | /v1/evaluations/{evaluation_id}/step |
| <a id="s-8b4aade718"></a>`provider_evidence` | None |
| <a id="s-5b5fb2662b"></a>`read_collection` | None |
| <a id="s-324c96b5c6"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations/{evaluation_id}/step](../http/post-v1-evaluations-evaluation-id-step.md)
- [stove0 evaluation step](../cli/stove0-evaluation-step.md)

## Governing policies

- <a id="pa-7a7854ec51"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-620a7edace"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-adf38cf989"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/128`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9156cd0b3ebbbcad96a357d21bb5a609b9ef213da79a21b4cd93c8b1288b9af8 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "evaluation step"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "step_evaluation",
  "path": "/v1/evaluations/{evaluation_id}/step",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
