# Operation parity: list_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-list-events:77134fed8b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `events` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["event list"] |
| `client` | Stove0ApiClient |
| `method` | GET |
| `operation_id` | list_events |
| `path` | /v1/events |
| `provider_evidence` | None |
| `read_collection` | {"cursor_parameter": "after", "kind": "cursor-feed", "limit_parameter": "limit"} |
| `response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/events](../http/get-v1-events.md)
- [stove0 event list](../cli/stove0-event-list.md)

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

- `/external_contract/operations/131`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 228f282bb54b7032346e272d028770a325fda12102aef4d7dae9f41f3b364a08 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "event list"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "list_events",
  "path": "/v1/events",
  "provider_evidence": null,
  "read_collection": {
    "cursor_parameter": "after",
    "kind": "cursor-feed",
    "limit_parameter": "limit"
  },
  "response_authority": "operator-projection"
}
```
