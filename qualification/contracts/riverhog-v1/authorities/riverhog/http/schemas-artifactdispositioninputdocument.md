# schemas: ArtifactDispositionInputDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositioninputdocument:bfb5c943ca -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionInputDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CanonicalRelPath](schemas-canonicalrelpath.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: ArtifactDispositionInputDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_root_sha256` | yes | string |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `path` | yes | #/components/schemas/CanonicalRelPath |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 18a9d379929f7e070761f6950cd137bfefaf471c2866f75d414b7faec3e731b0 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_root_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Archive Root Sha256",
      "type": "string"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    }
  },
  "required": [
    "collection_id",
    "archive_root_sha256",
    "path"
  ],
  "title": "ArtifactDispositionInputDocument",
  "type": "object"
}
```
