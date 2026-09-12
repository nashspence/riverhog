# schemas: RetirementClaimReferenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retirementclaimreferencedocument:83af135e41 -->

Exact claim evidence authorizing one retirement deletion plan.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-41ae99dfe3"></a>
- <a id="s-3f0c6f431f"></a>`title`: RetirementClaimReferenceDocument
- <a id="s-bf12ac752b"></a>`description`: Exact claim evidence authorizing one retirement deletion plan.
- <a id="s-0618482f13"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-61665e5e9e"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c71cfa0fc9"></a>`execution_id` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-77cbd0e0de"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-aeaffe0495"></a>`outcomes` | no | anyOf=#/components/schemas/ExactSetAuthorityDocument \| type="null" |  |
| <a id="s-b4b8f49228"></a>`output_collection_id` | no | anyOf=#/components/schemas/CollectionId \| type="null" |  |
| <a id="s-e9e363aaf6"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-39f7c3bf74"></a>[oneOf alternative 1 · field output_collection_id](#s-41ae99dfe3) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-61665e5e9e) | `length · characters · fixed` | shared above |
| <a id="s-5e31d6c1e5"></a>[field execution_id · string value](#s-c71cfa0fc9) | `length · characters · fixed` | shared above |
| [field work_id](#s-e9e363aaf6) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)

## Governing policies

- <a id="pa-a115916f5a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-bce01adfd9"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-489affe726"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetirementClaimReferenceDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7bf2593b59150209bcbb33413d688188b7cde9cae8da595c8bbd627946da79b -->

```json
{
  "additionalProperties": false,
  "description": "Exact claim evidence authorizing one retirement deletion plan.",
  "oneOf": [
    {
      "properties": {
        "execution_id": {
          "type": "string"
        },
        "outcomes": {
          "type": "null"
        },
        "output_collection_id": {
          "type": "integer"
        }
      },
      "required": [
        "execution_id",
        "output_collection_id"
      ]
    },
    {
      "properties": {
        "execution_id": {
          "type": "null"
        },
        "outcomes": {
          "type": "object"
        },
        "output_collection_id": {
          "type": "null"
        }
      },
      "required": [
        "outcomes"
      ]
    }
  ],
  "properties": {
    "claim_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Claim Id",
      "type": "string"
    },
    "execution_id": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Execution Id"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "outcomes": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ExactSetAuthorityDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "output_collection_id": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionId"
        },
        {
          "type": "null"
        }
      ]
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    }
  },
  "required": [
    "claim_id",
    "fence",
    "work_id"
  ],
  "title": "RetirementClaimReferenceDocument",
  "type": "object"
}
```
