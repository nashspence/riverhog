# Operation parity: list_processing_claim_disposition_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-processing-claim-di-7d7e9ee32e:62ff06b8e1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-56d2268bde"></a>
| Concern | Contract |
|---|---|
| <a id="s-3e4da804b0"></a>`application` | riverhog |
| <a id="s-5537b39b69"></a>`classification` | client-only-primitive |
| <a id="s-54b52db65a"></a>`cli_commands` | [] |
| <a id="s-e99656441b"></a>`client` | ApiClient |
| <a id="s-a106ca5c4e"></a>`method` | GET |
| <a id="s-519930c692"></a>`operation_id` | list_processing_claim_disposition_outputs |
| <a id="s-8f23a97099"></a>`path` | /v1/collection-processing-claims/{claim_id}/derivation/output-edges |
| <a id="s-4b2bcc820d"></a>`provider_evidence` | None |
| <a id="s-74254a438e"></a>`read_collection` | {"authority": "processing-claim-disposition-outputs", "authority_parameter": "authority_sha256", "cursor_parameter": "start_ordinal", "fixed_limit": 128, "kind": "exact-authority-page"} |
| <a id="s-b26e5e182c"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/derivation/output-edges](../http/get-v1-collection-processing-claims-claim-id-derivation-output-edges.md)

## Governing policies

- <a id="pa-e92abaace9"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-4747a3e82d"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-8fced943f4"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/34`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da28414a08f2fe8c6c158a7fc98077f759532eef1084a9e3732e88ef7810f7ec -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_processing_claim_disposition_outputs",
  "path": "/v1/collection-processing-claims/{claim_id}/derivation/output-edges",
  "provider_evidence": null,
  "read_collection": {
    "authority": "processing-claim-disposition-outputs",
    "authority_parameter": "authority_sha256",
    "cursor_parameter": "start_ordinal",
    "fixed_limit": 128,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```
