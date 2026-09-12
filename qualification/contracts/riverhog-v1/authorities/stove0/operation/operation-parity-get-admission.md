# Operation parity: get_admission

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-get-admission:e1958e2993 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [admissions](families/admissions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-dc85f724b2"></a>
| Concern | Contract |
|---|---|
| <a id="s-7b1975b035"></a>`application` | stove0 |
| <a id="s-c3b5ff5e1d"></a>`classification` | human-cli+json |
| <a id="s-6da8e416b6"></a>`cli_commands` | ["admission show"] |
| <a id="s-ab17aac88a"></a>`client` | Stove0ApiClient |
| <a id="s-a68cf1373d"></a>`method` | GET |
| <a id="s-3a7251957f"></a>`operation_id` | get_admission |
| <a id="s-7e5228db72"></a>`path` | /v1/admissions/{admission_id} |
| <a id="s-7a4c3d2ea2"></a>`provider_evidence` | None |
| <a id="s-09d2636142"></a>`read_collection` | None |
| <a id="s-fe9916bbb9"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/admissions/{admission_id}](../http/get-v1-admissions-admission-id.md)
- [stove0-client admission show](../../stove0-client/cli/stove0-client-admission-show.md)

## Governing policies

- <a id="pa-609ad444d7"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c5e21d624a"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-05e230302f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/122`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e3a42d0b21023ba9f33fceb4516991d3decb42d3ee253c5ce4c281364e7c23ef -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "admission show"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "get_admission",
  "path": "/v1/admissions/{admission_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
