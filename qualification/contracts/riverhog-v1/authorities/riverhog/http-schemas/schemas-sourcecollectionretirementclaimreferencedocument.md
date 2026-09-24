# schemas: SourceCollectionRetirementClaimReferenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-sourcecollectionretirementclaimre-c2e390d2a9:f53bf9c72e -->

Exact claim evidence authorizing one source collection deletion plan.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d67ec0d75b"></a>

- <a id="s-70bbded4d8"></a>`type`: `"object"`
- <a id="s-1cedfa0c67"></a>`additionalProperties`: `false`
- <a id="s-3851a24fb0"></a>`description`: `"Exact claim evidence authorizing one source collection deletion plan."`
- <a id="s-df6bce345a"></a>`required`: `["claim_id","fence","work_id"]`
- <a id="s-e126c0a5a6"></a>`title`: `"SourceCollectionRetirementClaimReferenceDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b28013b5b7"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Claim Id" |  |
| <a id="s-6c25603795"></a>`execution_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Execution Id" |  |
| <a id="s-cbb5ee37ed"></a>`fence` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-5ad08816e4"></a>`outcomes` | no | anyOf=[([ExactSetIdentityDocument](schemas-exactsetidentitydocument.md)); (type="null")] |  |
| <a id="s-d06292a7f7"></a>`output_collection_id` | no | anyOf=[([CollectionId](schemas-collectionid.md)); (type="null")] |  |
| <a id="s-a3faecc7ea"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `oneOf` alternative 1](#s-19deed0e71) |
| 2 | [See `oneOf` alternative 2](#s-159426b525) |

### <a id="s-19deed0e71"></a>`oneOf` alternative 1

- <a id="s-6cad2db48f"></a>`required`: `["execution_id","output_collection_id"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b2c9598b71"></a>`execution_id` | yes | type="string" |  |
| <a id="s-22be1f3371"></a>`outcomes` | no | type="null" |  |
| <a id="s-5f7022e620"></a>`output_collection_id` | yes | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |  |

### <a id="s-159426b525"></a>`oneOf` alternative 2

- <a id="s-d17cab06ca"></a>`required`: `["outcomes"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2132acb0ef"></a>`execution_id` | no | type="null" |  |
| <a id="s-9cd04fe469"></a>`outcomes` | yes | type="object" |  |
| <a id="s-cfaa509655"></a>`output_collection_id` | no | type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-b28013b5b7) | `length · characters · fixed` | shared above |
| <a id="s-a9e0eb692f"></a>[field execution_id · string value](#s-6c25603795) | `length · characters · fixed` | shared above |
| [field work_id](#s-a3faecc7ea) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)
- [ExactSetIdentityDocument](schemas-exactsetidentitydocument.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f32ed06009"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-21a9f05e58"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/SourceCollectionRetirementClaimReferenceDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8fd629b9bbbccf5fb0500f68a8b04f91b8bbb0f61d39715abf335ee42628fc1b -->

```json
{
  "additionalProperties": false,
  "description": "Exact claim evidence authorizing one source collection deletion plan.",
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
          "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
          "type": "string"
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
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "outcomes": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ExactSetIdentityDocument"
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
  "title": "SourceCollectionRetirementClaimReferenceDocument",
  "type": "object"
}
```

</details>
