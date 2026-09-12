# Operation parity: create_or_resume_archive_copy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-create-or-resume-archive-copy:d6aeda2ae2 -->

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
| `cli_commands` | ["archive copy start"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | create_or_resume_archive_copy |
| `path` | /v1/archive/copies |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/archive/copies](../http/post-v1-archive-copies.md)
- [piggity archive copy start](../../piggity/cli/piggity-archive-copy-start.md)

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

- `/external_contract/operations/13`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e7c4963425ffef94099b0c90954ef0c94d99c9588d62a81f9311aa38b282afa -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive copy start"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "create_or_resume_archive_copy",
  "path": "/v1/archive/copies",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
