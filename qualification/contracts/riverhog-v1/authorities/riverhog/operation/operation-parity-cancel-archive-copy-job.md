# Operation parity: cancel_archive_copy_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-cancel-archive-copy-job:10b40b9177 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `archive` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["archive copy cancel"] |
| `client` | ApiClient |
| `method` | DELETE |
| `operation_id` | cancel_archive_copy_job |
| `path` | /v1/archive/copies/{collection_id}/{destination_store} |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [DELETE /v1/archive/copies/{collection_id}/{destination_store}](../http/delete-v1-archive-copies-collection-id-destination-store.md)
- [piggity archive copy cancel](../../piggity/cli/piggity-archive-copy-cancel.md)

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

- `/external_contract/operations/16`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da66bcb27abaea166a31f55049f3ecee442e520e16352e7e5b772f5f688319c5 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive copy cancel"
  ],
  "client": "ApiClient",
  "method": "DELETE",
  "operation_id": "cancel_archive_copy_job",
  "path": "/v1/archive/copies/{collection_id}/{destination_store}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
