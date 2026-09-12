# Operation parity: get_collection_provenance_verification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-collection-provenanc-292ae4cdc5:b917ccacc1 -->

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
| `cli_commands` | ["collection provenance verification-show", "collection provenance verify"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | get_collection_provenance_verification |
| `path` | /v1/collections/{collection_id}/provenance/verification |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/verification](../http/get-v1-collections-collection-id-provenance-verification.md)
- [piggity collection provenance verification-show](../../piggity/cli/piggity-collection-provenance-verification-show.md)
- [piggity collection provenance verify](../../piggity/cli/piggity-collection-provenance-verify.md)

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

- `/external_contract/operations/84`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b588b438e2903cc45f0207d3566a499f32975bcc5fb4c335ff0b9ff3f6d167b6 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection provenance verification-show",
    "collection provenance verify"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_collection_provenance_verification",
  "path": "/v1/collections/{collection_id}/provenance/verification",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
