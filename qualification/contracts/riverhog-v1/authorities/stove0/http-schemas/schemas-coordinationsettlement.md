# schemas: CoordinationSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-coordinationsettlement:3793517842 -->

Success-only exact completion of one root or branch-bound coordinator.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d0ceeae978"></a>

- <a id="s-c4f7fee2d0"></a>`type`: `"object"`
- <a id="s-5d7d2807e6"></a>`additionalProperties`: `false`
- <a id="s-3d0c1a6a86"></a>`description`: `"Success-only exact completion of one root or branch-bound coordinator."`
- <a id="s-11ece5be38"></a>`required`: `["work","branch_set_sha256","children","contains_external_effects","settlement_sha256"]`
- <a id="s-15bb1ea7e0"></a>`title`: `"CoordinationSettlement"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-00b0c65a2e"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Branch Set Sha256" |  |
| <a id="s-d20e730907"></a>`children` | yes | type="array"; items=([CoordinationChildSettlementRef](schemas-coordinationchildsettlementref.md)); title="Children" |  |
| <a id="s-22e70e8b9b"></a>`collection_result` | no | anyOf=[([CoordinationCollectionResult](schemas-coordinationcollectionresult.md)); (type="null")] |  |
| <a id="s-52f40f13e4"></a>`contains_external_effects` | yes | type="boolean"; title="Contains External Effects" |  |
| <a id="s-0e942bf28a"></a>`final_join_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Final Join Settlement Sha256" |  |
| <a id="s-f3bd3005e5"></a>`format` | no | type="string"; const="stove0-coordination-settlement/v1"; default="stove0-coordination-settlement/v1"; title="Format" |  |
| <a id="s-5432273c79"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Settlement Sha256" |  |
| <a id="s-ea2c242f18"></a>`work` | yes | [WorkIdentity](schemas-workidentity.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field children](#s-d20e730907) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_set_sha256](#s-00b0c65a2e) | `length · characters · fixed` | shared above |
| <a id="s-7cf3dca2f0"></a>[field final_join_settlement_sha256 · string value](#s-0e942bf28a) | `length · characters · fixed` | shared above |
| [field settlement_sha256](#s-5432273c79) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CoordinationChildSettlementRef](schemas-coordinationchildsettlementref.md)
- [CoordinationCollectionResult](schemas-coordinationcollectionresult.md)
- [WorkIdentity](schemas-workidentity.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-209aaa37c3"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-ad5f2311b6"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-aff7594db8"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CoordinationSettlement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5fec61eb1a46933a79e3188e2861715cc83ed7e3bfe0c779e2fac77d41e13d59 -->

```json
{
  "additionalProperties": false,
  "description": "Success-only exact completion of one root or branch-bound coordinator.",
  "properties": {
    "branch_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Branch Set Sha256",
      "type": "string"
    },
    "children": {
      "items": {
        "$ref": "#/components/schemas/CoordinationChildSettlementRef"
      },
      "title": "Children",
      "type": "array"
    },
    "collection_result": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CoordinationCollectionResult"
        },
        {
          "type": "null"
        }
      ]
    },
    "contains_external_effects": {
      "title": "Contains External Effects",
      "type": "boolean"
    },
    "final_join_settlement_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Final Join Settlement Sha256"
    },
    "format": {
      "const": "stove0-coordination-settlement/v1",
      "default": "stove0-coordination-settlement/v1",
      "title": "Format",
      "type": "string"
    },
    "settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Settlement Sha256",
      "type": "string"
    },
    "work": {
      "$ref": "#/components/schemas/WorkIdentity"
    }
  },
  "required": [
    "work",
    "branch_set_sha256",
    "children",
    "contains_external_effects",
    "settlement_sha256"
  ],
  "title": "CoordinationSettlement",
  "type": "object"
}
```

</details>
