# Operation parity: get_archive_copy_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-archive-copy-job:6b18b3794b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `archive` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/17`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/archive/copies/{collection_id}/{destination_store}](../http/get-v1-archive-copies-collection-id-destination-store.md)
- [piggity archive copy show](../../piggity/cli/piggity-archive-copy-show.md)
- [piggity archive copy watch](../../piggity/cli/piggity-archive-copy-watch.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["archive copy show", "archive copy watch"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | get_archive_copy_job |
| `path` | /v1/archive/copies/{collection_id}/{destination_store} |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74e8a128af8f5b244f14163eb363c7b0a2171ee853fbda6236776af5e061d260 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive copy show",
    "archive copy watch"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_archive_copy_job",
  "path": "/v1/archive/copies/{collection_id}/{destination_store}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
