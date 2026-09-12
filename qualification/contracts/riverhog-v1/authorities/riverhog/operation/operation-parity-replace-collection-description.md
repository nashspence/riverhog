# Operation parity: replace_collection_description

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-replace-collection-description:1d61b9e54f -->

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
| `cli_commands` | ["collection describe"] |
| `client` | ApiClient |
| `method` | PUT |
| `operation_id` | replace_collection_description |
| `path` | /v1/collections/{collection_id}/description |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [PUT /v1/collections/{collection_id}/description](../http/put-v1-collections-collection-id-description.md)
- [piggity collection describe](../../piggity/cli/piggity-collection-describe.md)

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

- `/external_contract/operations/76`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dfb4f8db18d330335224d579c3b794b62d8331c2c992379113ac55bc32cc515f -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection describe"
  ],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "replace_collection_description",
  "path": "/v1/collections/{collection_id}/description",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
