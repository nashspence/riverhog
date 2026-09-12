# Operation parity: add_collection_tag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-add-collection-tag:d1606a8eeb -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/87`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/collections/{collection_id}/tags:add](../http/post-v1-collections-collection-id-tags-add.md)
- [piggity collection tag add](../../piggity/cli/piggity-collection-tag-add.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection tag add"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | add_collection_tag |
| `path` | /v1/collections/{collection_id}/tags:add |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d9f0375da20a722b152ef6ef271d52014df79b1f3d848cadfd6ce3c6b2a6e59 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection tag add"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "add_collection_tag",
  "path": "/v1/collections/{collection_id}/tags:add",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
