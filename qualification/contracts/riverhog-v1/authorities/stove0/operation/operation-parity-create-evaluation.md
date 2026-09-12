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

<a id="s-7839cc8af8"></a>
| Concern | Contract |
|---|---|
| <a id="s-37891d3ea9"></a>`application` | stove0 |
| <a id="s-04d0979c66"></a>`classification` | human-cli+json |
| <a id="s-a8428db5e5"></a>`cli_commands` | ["evaluation create"] |
| <a id="s-85a515d17f"></a>`client` | Stove0ApiClient |
| <a id="s-aaa5d42a60"></a>`method` | POST |
| <a id="s-aed7cd161d"></a>`operation_id` | create_evaluation |
| <a id="s-0058dddf19"></a>`path` | /v1/evaluations |
| <a id="s-9cabe5b74c"></a>`provider_evidence` | None |
| <a id="s-849ee5e405"></a>`read_collection` | None |
| <a id="s-5e02316aa6"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations](../http/post-v1-evaluations.md)
- [stove0 evaluation create](../cli/stove0-evaluation-create.md)

## Governing policies

- <a id="pa-1129a1996a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c32dd52abf"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-fedb9c1e2d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

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
