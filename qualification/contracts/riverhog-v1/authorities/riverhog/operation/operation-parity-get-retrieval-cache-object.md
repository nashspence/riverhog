# Operation parity: get_retrieval_cache_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-retrieval-cache-object:001ab51ffb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `retrieval-cache` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["retrieval cache show"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | get_retrieval_cache_object |
| `path` | /v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id} |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id}](../http/get-v1-retrieval-cache-objects-collection-id-source-store-object-id.md)
- [piggity retrieval cache show](../../piggity/cli/piggity-retrieval-cache-show.md)

## Governing policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/95`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 80bb7a9afd828c148050dd5685a715e8cfcfe32e6ada8683bbeabc0d0fd5281a -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "retrieval cache show"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_retrieval_cache_object",
  "path": "/v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
