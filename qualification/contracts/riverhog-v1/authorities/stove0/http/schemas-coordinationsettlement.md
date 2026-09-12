# schemas: CoordinationSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-coordinationsettlement:27a57daf18 -->

Success-only exact completion of one root or branch-bound coordinator.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-d0ceeae9784f"></a>
- <a id="s-15bb1ea7e02d"></a>`title`: CoordinationSettlement
- <a id="s-3d0c1a6a8616"></a>`description`: Success-only exact completion of one root or branch-bound coordinator.
- <a id="s-c4f7fee2d0d5"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-00b0c65a2ea3"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d20e730907c1"></a>`children` | yes | type="array"; items=(#/components/schemas/CoordinationChildSettlementRef) |  |
| <a id="s-22e70e8b9bd2"></a>`collection_result` | no | anyOf=#/components/schemas/CoordinationCollectionResult \| type="null" |  |
| <a id="s-52f40f13e478"></a>`contains_external_effects` | yes | type="boolean" |  |
| <a id="s-0e942bf28a50"></a>`final_join_settlement_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-f3bd3005e5f4"></a>`format` | no | type="string"; const="stove0-coordination-settlement/v1" |  |
| <a id="s-5432273c79a5"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ea2c242f18ce"></a>`work` | yes | #/components/schemas/WorkIdentity |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field children](#s-d20e730907c1) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_set_sha256](#s-00b0c65a2ea3) | `length · characters · fixed` | shared above |
| <a id="s-7cf3dca2f025"></a>field final_join_settlement_sha256 · anyOf alternative 1 | `length · characters · fixed` | shared above |
| [field settlement_sha256](#s-5432273c79a5) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CoordinationChildSettlementRef](schemas-coordinationchildsettlementref.md)
- [schemas: CoordinationCollectionResult](schemas-coordinationcollectionresult.md)
- [schemas: WorkIdentity](schemas-workidentity.md)

## Governing policies

- <a id="pa-29d102a67662"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-02bb21e5dc4e"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-8d9578d7bb37"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CoordinationSettlement`

### Exact owned JSON

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
