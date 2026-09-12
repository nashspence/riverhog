# Operation parity: trace_collection_file_provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-trace-collection-file-provenance:32dc3492aa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4083903f39"></a>
| Concern | Contract |
|---|---|
| <a id="s-15ec8708b9"></a>`application` | riverhog |
| <a id="s-16d262d6ae"></a>`classification` | human-cli+json |
| <a id="s-e312dc5104"></a>`cli_commands` | ["collection provenance trace"] |
| <a id="s-4ed0f6677a"></a>`client` | ApiClient |
| <a id="s-0a24afdb0c"></a>`method` | GET |
| <a id="s-40e0dd2fb6"></a>`operation_id` | trace_collection_file_provenance |
| <a id="s-ed19380021"></a>`path` | /v1/collections/{collection_id}/provenance/trace/{path} |
| <a id="s-30d765a9d6"></a>`provider_evidence` | None |
| <a id="s-39ff8ae09c"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-ed84b9438a"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/trace/{path}](../http/get-v1-collections-collection-id-provenance-trace-path.md)
- [piggity collection provenance trace](../../piggity/cli/piggity-collection-provenance-trace.md)

## Governing policies

- <a id="pa-48a5c32ae3"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-a0e1fead12"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-57dd159b2f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/82`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 52773897eec65fd9c2f8e57a2c522a6efe789490b2d68d6ad652239615f0a519 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection provenance trace"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "trace_collection_file_provenance",
  "path": "/v1/collections/{collection_id}/provenance/trace/{path}",
  "provider_evidence": null,
  "read_collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  },
  "response_authority": "http-json"
}
```
