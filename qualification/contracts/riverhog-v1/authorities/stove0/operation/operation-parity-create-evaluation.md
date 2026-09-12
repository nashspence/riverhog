# Operation parity: create_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-create-evaluation:44546c282c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [evaluations](families/evaluations/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7839cc8af84f"></a>
| Concern | Contract |
|---|---|
| <a id="s-37891d3ea974"></a>`application` | stove0 |
| <a id="s-04d0979c665b"></a>`classification` | human-cli+json |
| <a id="s-a8428db5e55c"></a>`cli_commands` | ["evaluation create"] |
| <a id="s-85a515d17f6b"></a>`client` | Stove0ApiClient |
| <a id="s-aaa5d42a608b"></a>`method` | POST |
| <a id="s-aed7cd161daa"></a>`operation_id` | create_evaluation |
| <a id="s-0058dddf198a"></a>`path` | /v1/evaluations |
| <a id="s-9cabe5b74c18"></a>`provider_evidence` | None |
| <a id="s-849ee5e405d6"></a>`read_collection` | None |
| <a id="s-5e02316aa6e8"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations](../http/post-v1-evaluations.md)
- [stove0 evaluation create](../cli/stove0-evaluation-create.md)

## Governing policies

- <a id="pa-1129a1996a74"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-c32dd52abf10"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-fedb9c1e2d67"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/125`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12e9bfc10407812b8facc18d4b76500a002fd71e033371806d2c33ebfb87cd85 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "evaluation create"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "create_evaluation",
  "path": "/v1/evaluations",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
