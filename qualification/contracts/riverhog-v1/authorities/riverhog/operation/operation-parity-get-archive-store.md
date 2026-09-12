# Operation parity: get_archive_store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-archive-store:a06f6b9b81 -->

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
| `cli_commands` | ["archive store show"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | get_archive_store |
| `path` | /v1/archive/stores/{store} |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/archive/stores/{store}](../http/get-v1-archive-stores-store.md)
- [piggity archive store show](../../piggity/cli/piggity-archive-store-show.md)

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

- `/external_contract/operations/19`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 71a948d7a22290a138112f74eeb39455b2f2fefdc5ea52a1041a9e8572fa62da -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive store show"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_archive_store",
  "path": "/v1/archive/stores/{store}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
