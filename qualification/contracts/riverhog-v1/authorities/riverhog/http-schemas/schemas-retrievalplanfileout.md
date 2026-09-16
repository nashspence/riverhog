# schemas: RetrievalPlanFileOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalplanfileout:d72c3dfd79 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-3204df2fc3"></a>

- <a id="s-8e3814d88d"></a>`type`: `"object"`
- <a id="s-2f1216f422"></a>`additionalProperties`: `false`
- <a id="s-89d59628d5"></a>`required`: `["path","bytes","sha256","collection_id","requires_restore"]`
- <a id="s-cbd43d9a68"></a>`title`: `"RetrievalPlanFileOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-38bb68239d"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-02b7fe1b76"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-2b5bab20c8"></a>`path` | yes | #/components/schemas/CanonicalRelPath |  |
| <a id="s-a5ea03a2ad"></a>`requires_restore` | yes | type="boolean" |  |
| <a id="s-fc277b5d87"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-38bb68239d) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-fc277b5d87) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-3cd8fd46e8"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-4c9f7f3f71"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-a7fb6949ea"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanFileOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e691d6f9eba92c150d3318be4a56e5c404238f52d7d7703491302d2d21b01519 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    },
    "requires_restore": {
      "title": "Requires Restore",
      "type": "boolean"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "path",
    "bytes",
    "sha256",
    "collection_id",
    "requires_restore"
  ],
  "title": "RetrievalPlanFileOut",
  "type": "object"
}
```

</details>
