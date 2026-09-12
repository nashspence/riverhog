# Operation parity: plan_archive_copy_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-plan-archive-copy-retirement:c370035def -->

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
| `operation_id` | plan_archive_copy_retirement |
| `path` | /v1/archive/copies/retirement-plan |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/archive/copies/retirement-plan](../http/post-v1-archive-copies-retirement-plan.md)
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

- `/external_contract/operations/15`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5140b4139f0920dc71be4e41a6dcda6ad290e8defcca0d375136a4342ebab44d -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive retire"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "plan_archive_copy_retirement",
  "path": "/v1/archive/copies/retirement-plan",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
