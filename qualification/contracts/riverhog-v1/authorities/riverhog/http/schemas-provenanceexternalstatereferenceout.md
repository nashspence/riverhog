# schemas: ProvenanceExternalStateReferenceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-provenanceexternalstatereferenceout:27323ab20c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceExternalStateReferenceOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: ProvenanceExternalStateReferenceOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `entry_id` | yes | #/components/schemas/ProvenanceEntryId |  |
| `entry_json_sha256` | yes | string |  |
| `from_journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
| `state_id` | yes | #/components/schemas/ProvenanceStateId |  |
| `to_journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
