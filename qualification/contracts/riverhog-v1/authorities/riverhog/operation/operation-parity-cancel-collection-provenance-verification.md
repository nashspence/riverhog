# Operation parity: cancel_collection_provenance_verification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-cancel-collection-proven-9adb6dc042:e3e154f60d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection provenance verification-cancel"] |
| `client` | ApiClient |
| `method` | DELETE |
| `operation_id` | cancel_collection_provenance_verification |
| `path` | /v1/collections/{collection_id}/provenance/verification |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [DELETE /v1/collections/{collection_id}/provenance/verification](../http/delete-v1-collections-collection-id-provenance-verification.md)
- [piggity collection provenance verification-cancel](../../piggity/cli/piggity-collection-provenance-verification-cancel.md)

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

- `/external_contract/operations/83`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 224aa036ea4bd10ed5d542d08485c0a864b7859e6227caddf17e2694e2d14664 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection provenance verification-cancel"
  ],
  "client": "ApiClient",
  "method": "DELETE",
  "operation_id": "cancel_collection_provenance_verification",
  "path": "/v1/collections/{collection_id}/provenance/verification",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
