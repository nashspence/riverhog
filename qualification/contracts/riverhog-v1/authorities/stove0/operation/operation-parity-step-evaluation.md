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

<a id="s-27b7249254aa"></a>
| Concern | Contract |
|---|---|
| <a id="s-c024337f6e18"></a>`application` | stove0 |
| <a id="s-c47db2c7eafd"></a>`classification` | human-cli+json |
| <a id="s-ee0060c6e483"></a>`cli_commands` | ["evaluation step"] |
| <a id="s-57f7c91eaab2"></a>`client` | Stove0ApiClient |
| <a id="s-3c2a7616517a"></a>`method` | POST |
| <a id="s-dc7025bb3858"></a>`operation_id` | step_evaluation |
| <a id="s-11819584527a"></a>`path` | /v1/evaluations/{evaluation_id}/step |
| <a id="s-8b4aade71832"></a>`provider_evidence` | None |
| <a id="s-5b5fb2662b6f"></a>`read_collection` | None |
| <a id="s-324c96b5c6c2"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations/{evaluation_id}/step](../http/post-v1-evaluations-evaluation-id-step.md)
- [stove0 evaluation step](../cli/stove0-evaluation-step.md)

## Governing policies

- <a id="pa-7a7854ec51d7"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-620a7edacea4"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-adf38cf98951"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

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
