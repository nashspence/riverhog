# schemas: RecipeRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-reciperef:8e602c9c7b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-62017ec1c8"></a>

- <a id="s-9440c1834c"></a>`type`: `"object"`
- <a id="s-5d706d6d38"></a>`additionalProperties`: `false`
- <a id="s-3f565947e6"></a>`required`: `["id","revision","sha256"]`
- <a id="s-4f771980df"></a>`title`: `"RecipeRef"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7a8a87bc51"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-a4b73aecf5"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |
| <a id="s-f3f07db30c"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-f3f07db30c) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-592bca4bba"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-fa9aa07bab"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeRef`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3790d8f811ebb13fed76cbd0a115a818961abe76707214c58706e9033077b650 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "revision": {
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "id",
    "revision",
    "sha256"
  ],
  "title": "RecipeRef",
  "type": "object"
}
```

</details>
