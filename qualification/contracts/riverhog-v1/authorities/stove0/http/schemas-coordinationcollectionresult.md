# schemas: CoordinationCollectionResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-coordinationcollectionresult:63aad8c924 -->

Parent-visible collection produced by the coordinator's actual join leaf.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-a58a7320a146"></a>
- <a id="s-82a4310f270b"></a>`title`: CoordinationCollectionResult
- <a id="s-957b57059ee3"></a>`description`: Parent-visible collection produced by the coordinator's actual join leaf.
- <a id="s-8e5422d43a8a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c43655809627"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-23d790639fa1"></a>`join_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ee6a9be5607c"></a>`output_collection` | yes | #/components/schemas/CollectionRootRef |  |
| <a id="s-c64a7de33177"></a>`output_selection` | yes | #/components/schemas/ArtifactSelectionRef |  |
| <a id="s-7f67d120de39"></a>`producer_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field derivation_sha256](#s-c43655809627) | `length · characters · fixed` | shared above |
| [field join_settlement_sha256](#s-23d790639fa1) | `length · characters · fixed` | shared above |
| [field producer_work_id](#s-7f67d120de39) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactSelectionRef](schemas-artifactselectionref.md)
- [schemas: CollectionRootRef](schemas-collectionrootref.md)

## Governing policies

- <a id="pa-62369d16c530"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-c22c7922f0e1"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CoordinationCollectionResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 820ceced08fd62e760ccc86af09c0c0853234f5a2890ee7e56de01a2420d37ce -->

```json
{
  "additionalProperties": false,
  "description": "Parent-visible collection produced by the coordinator's actual join leaf.",
  "properties": {
    "derivation_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Derivation Sha256",
      "type": "string"
    },
    "join_settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Join Settlement Sha256",
      "type": "string"
    },
    "output_collection": {
      "$ref": "#/components/schemas/CollectionRootRef"
    },
    "output_selection": {
      "$ref": "#/components/schemas/ArtifactSelectionRef"
    },
    "producer_work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Producer Work Id",
      "type": "string"
    }
  },
  "required": [
    "producer_work_id",
    "join_settlement_sha256",
    "derivation_sha256",
    "output_collection",
    "output_selection"
  ],
  "title": "CoordinationCollectionResult",
  "type": "object"
}
```
