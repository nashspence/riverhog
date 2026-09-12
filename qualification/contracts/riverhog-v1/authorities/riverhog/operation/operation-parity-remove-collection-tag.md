# Operation parity: remove_collection_tag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-remove-collection-tag:9e49a0970e -->

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
| `cli_commands` | ["collection tag remove"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | remove_collection_tag |
| `path` | /v1/collections/{collection_id}/tags:remove |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collections/{collection_id}/tags:remove](../http/post-v1-collections-collection-id-tags-remove.md)
- [piggity collection tag remove](../../piggity/cli/piggity-collection-tag-remove.md)

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

- `/external_contract/operations/89`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d74063464c07f8a3706cacbfce144cc2c3e669aefb80e05239bb45ceb5e5a513 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection tag remove"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "remove_collection_tag",
  "path": "/v1/collections/{collection_id}/tags:remove",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
