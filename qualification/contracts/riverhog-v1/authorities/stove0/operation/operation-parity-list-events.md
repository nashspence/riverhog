# Operation parity: list_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-list-events:77134fed8b -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `events` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/131`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/events](../http/get-v1-events.md)
- [stove0 event list](../cli/stove0-event-list.md)

## Contract

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
