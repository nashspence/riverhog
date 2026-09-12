# Operation parity: head_collection_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-head-collection-provenance-journal:656f831a47 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/80`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | standard-tool/protocol |
| `cli_commands` | [] |
| `client` | None |
| `method` | HEAD |
| `operation_id` | head_collection_provenance_journal |
| `path` | /v1/collections/{collection_id}/provenance/journals/{journal_id} |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | stream-or-empty |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2053e6c469b6a0ef8143dd5506bb3e747cca16c2cef5c28c1c751513b7edbb33 -->

```json
{
  "application": "riverhog",
  "classification": "standard-tool/protocol",
  "cli_commands": [],
  "client": null,
  "method": "HEAD",
  "operation_id": "head_collection_provenance_journal",
  "path": "/v1/collections/{collection_id}/provenance/journals/{journal_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "stream-or-empty"
}
```
