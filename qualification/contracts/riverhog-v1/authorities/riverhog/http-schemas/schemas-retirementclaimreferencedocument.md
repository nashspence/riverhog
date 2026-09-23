# schemas: RetirementClaimReferenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retirementclaimreferencedocument:577a0a9d80 -->

Exact claim evidence authorizing one retirement deletion plan.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-41ae99dfe3"></a>

- <a id="s-0618482f13"></a>`type`: `"object"`
- <a id="s-60c80365a9"></a>`additionalProperties`: `false`
- <a id="s-bf12ac752b"></a>`description`: `"Exact claim evidence authorizing one retirement deletion plan."`
- <a id="s-221af02e54"></a>`required`: `["claim_id","fence","work_id"]`
- <a id="s-3f0c6f431f"></a>`title`: `"RetirementClaimReferenceDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-61665e5e9e"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Claim Id" |  |
| <a id="s-c71cfa0fc9"></a>`execution_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Execution Id" |  |
| <a id="s-77cbd0e0de"></a>`fence` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-aeaffe0495"></a>`outcomes` | no | anyOf=[([ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)); (type="null")] |  |
| <a id="s-b4b8f49228"></a>`output_collection_id` | no | anyOf=[([CollectionId](schemas-collectionid.md)); (type="null")] |  |
| <a id="s-e9e363aaf6"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `oneOf` alternative 1](#s-d900f9d126) |
| 2 | [See `oneOf` alternative 2](#s-33fc5cf119) |

### <a id="s-d900f9d126"></a>`oneOf` alternative 1

- <a id="s-4c64a0b8d5"></a>`required`: `["execution_id","output_collection_id"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7bcd44a05c"></a>`execution_id` | yes | type="string" |  |
| <a id="s-12f7ce203e"></a>`outcomes` | no | type="null" |  |
| <a id="s-39f7c3bf74"></a>`output_collection_id` | yes | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |  |

### <a id="s-33fc5cf119"></a>`oneOf` alternative 2

- <a id="s-48f37f74c7"></a>`required`: `["outcomes"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6cf04c2010"></a>`execution_id` | no | type="null" |  |
| <a id="s-45c45c44dd"></a>`outcomes` | yes | type="object" |  |
| <a id="s-c235f69279"></a>`output_collection_id` | no | type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-61665e5e9e) | `length · characters · fixed` | shared above |
| <a id="s-5e31d6c1e5"></a>[field execution_id · string value](#s-c71cfa0fc9) | `length · characters · fixed` | shared above |
| [field work_id](#s-e9e363aaf6) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)
- [ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3e64ab0c41"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-5681110271"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetirementClaimReferenceDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e631f1e8d7ccdd8ca043f074c9a257af80fdae98cae6476f800c25dccb01b335 -->

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

</details>
