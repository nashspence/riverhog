# Operation parity: get_collection_file_provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-collection-file-provenance:42585e3bff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection provenance show"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | get_collection_file_provenance |
| `path` | /v1/collections/{collection_id}/provenance/files/{path} |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/files/{path}](../http/get-v1-collections-collection-id-provenance-files-path.md)
- [piggity collection provenance show](../../piggity/cli/piggity-collection-provenance-show.md)

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

- `/external_contract/operations/78`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dab9ba3f33afa5f2615082519fd2a680c2b8b310099bf62942db8b1bcb71f108 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection provenance show"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_collection_file_provenance",
  "path": "/v1/collections/{collection_id}/provenance/files/{path}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
