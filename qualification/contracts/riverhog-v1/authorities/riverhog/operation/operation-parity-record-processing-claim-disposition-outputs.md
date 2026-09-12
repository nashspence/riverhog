# Operation parity: record_processing_claim_disposition_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-record-processing-claim-b22805a2c3:749200f58e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ba9100e6857e"></a>
| Concern | Contract |
|---|---|
| <a id="s-d0770d675763"></a>`application` | riverhog |
| <a id="s-9214550f95c8"></a>`classification` | client-only-primitive |
| <a id="s-526e8ff0df09"></a>`cli_commands` | [] |
| <a id="s-fb0794d32c20"></a>`client` | ApiClient |
| <a id="s-89db2515a51a"></a>`method` | PUT |
| <a id="s-5086b8aa3873"></a>`operation_id` | record_processing_claim_disposition_outputs |
| <a id="s-74f64858ede6"></a>`path` | /v1/collection-processing-claims/{claim_id}/derivation/output-edges |
| <a id="s-c70d61776716"></a>`provider_evidence` | None |
| <a id="s-c637385df3bf"></a>`read_collection` | None |
| <a id="s-56a27306a34e"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-processing-claims/{claim_id}/derivation/output-edges](../http/put-v1-collection-processing-claims-claim-id-derivation-output-edges.md)

## Governing policies

- <a id="pa-ed7f88749590"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-370b4e220b0e"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-c6fe82144789"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/35`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cbb3219d34095c19a74f68b9500b08a01f8ea43c2bc564a3de783f123ad9fec8 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "record_processing_claim_disposition_outputs",
  "path": "/v1/collection-processing-claims/{claim_id}/derivation/output-edges",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
