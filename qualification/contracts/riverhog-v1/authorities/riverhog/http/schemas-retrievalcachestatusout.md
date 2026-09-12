# schemas: RetrievalCacheStatusOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcachestatusout:5e6012674f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-d4cd078234e7"></a>
- <a id="s-a8b54722f89e"></a>`title`: RetrievalCacheStatusOut
- <a id="s-4b18d0211db9"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-465ff42a9eb2"></a>`configured` | yes | type="boolean" |  |
| <a id="s-c6e56b6d72c1"></a>`new_archive_enabled` | yes | type="boolean" |  |
| <a id="s-c9532554b898"></a>`objects` | yes | type="integer" |  |
| <a id="s-02c055a6230b"></a>`policy` | yes | #/components/schemas/RetrievalCachePolicyOut |  |
| <a id="s-e00db8b2312c"></a>`protected_objects` | yes | type="integer" |  |
| <a id="s-579f91048cc9"></a>`stored_bytes` | yes | type="integer" |  |
| <a id="s-5f68891811f2"></a>`stores` | yes | type="array"; items=(#/components/schemas/RetrievalCacheStoreStatusOut) |  |
| <a id="s-04f46513ac0b"></a>`unleased_objects` | yes | type="integer" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field stores](#s-5f68891811f2) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: RetrievalCachePolicyOut](schemas-retrievalcachepolicyout.md)
- [schemas: RetrievalCacheStoreStatusOut](schemas-retrievalcachestorestatusout.md)

## Governing policies

- <a id="pa-aea1cc96ef04"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-7f8f86b8a8a8"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheStatusOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61092d75a6bc2c2885c00ee6210558fcbf09b31f3414181c97fc286a499c4073 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "configured": {
      "title": "Configured",
      "type": "boolean"
    },
    "new_archive_enabled": {
      "title": "New Archive Enabled",
      "type": "boolean"
    },
    "objects": {
      "title": "Objects",
      "type": "integer"
    },
    "policy": {
      "$ref": "#/components/schemas/RetrievalCachePolicyOut"
    },
    "protected_objects": {
      "title": "Protected Objects",
      "type": "integer"
    },
    "stored_bytes": {
      "title": "Stored Bytes",
      "type": "integer"
    },
    "stores": {
      "items": {
        "$ref": "#/components/schemas/RetrievalCacheStoreStatusOut"
      },
      "title": "Stores",
      "type": "array"
    },
    "unleased_objects": {
      "title": "Unleased Objects",
      "type": "integer"
    }
  },
  "required": [
    "configured",
    "new_archive_enabled",
    "objects",
    "stored_bytes",
    "protected_objects",
    "unleased_objects",
    "stores",
    "policy"
  ],
  "title": "RetrievalCacheStatusOut",
  "type": "object"
}
```
