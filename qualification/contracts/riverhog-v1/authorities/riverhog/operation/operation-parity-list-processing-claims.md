# Operation parity: list_processing_claims

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-processing-claims:bbb8da4c68 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f3c262cc13f0"></a>
| Concern | Contract |
|---|---|
| <a id="s-b67423bd1976"></a>`application` | riverhog |
| <a id="s-416f48816dc6"></a>`classification` | client-only-primitive |
| <a id="s-766bcc7915f6"></a>`cli_commands` | [] |
| <a id="s-38bd63b4b410"></a>`client` | ApiClient |
| <a id="s-170332f0b269"></a>`method` | GET |
| <a id="s-c2fbe92dc646"></a>`operation_id` | list_processing_claims |
| <a id="s-4bb06436ce0e"></a>`path` | /v1/collection-processing-claims |
| <a id="s-1c1e53a65e58"></a>`provider_evidence` | None |
| <a id="s-ada0bcbeef59"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-2c9b64a9c179"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims](../http/get-v1-collection-processing-claims.md)

## Governing policies

- <a id="pa-53c647971c0d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-881e251de332"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-dc4092e15de1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/24`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9dc579bf87a37a03b13c4bc3c792d36f1d428a896358d0790dcdce63465767e3 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_processing_claims",
  "path": "/v1/collection-processing-claims",
  "provider_evidence": null,
  "read_collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  },
  "response_authority": "canonical-document"
}
```
