# Operation parity: retire_archive_copy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-retire-archive-copy:83a54b3e0b -->

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
| `cli_commands` | ["archive retire"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | retire_archive_copy |
| `path` | /v1/archive/copies/retire |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/archive/copies/retire](../http/post-v1-archive-copies-retire.md)
- [piggity archive retire](../../piggity/cli/piggity-archive-retire.md)

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

- `/external_contract/operations/14`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d44330ae27027863e122abb5d442c3d5895aca64a4696b9dadf3ba9eaf6c4b8 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive retire"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "retire_archive_copy",
  "path": "/v1/archive/copies/retire",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
