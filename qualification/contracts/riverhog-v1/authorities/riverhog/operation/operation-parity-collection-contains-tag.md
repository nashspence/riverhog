# Operation parity: collection_contains_tag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-collection-contains-tag:2a70c994a5 -->

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
| `cli_commands` | ["collection tag contains"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | collection_contains_tag |
| `path` | /v1/collections/{collection_id}/tags:contains |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/tags:contains](../http/get-v1-collections-collection-id-tags-contains.md)
- [piggity collection tag contains](../../piggity/cli/piggity-collection-tag-contains.md)

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

- `/external_contract/operations/88`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 666571d1cacf9d4490a0982374a5c878043ad7cf37fee56a573c23b7db847cb3 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection tag contains"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "collection_contains_tag",
  "path": "/v1/collections/{collection_id}/tags:contains",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
