# schemas: JoinSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-joinsettlement:077503a3f4 -->

Success-only, Riverhog-verified result of one resolved join plan.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-90043d6d0e"></a>

- <a id="s-b8d396c35b"></a>`type`: `"object"`
- <a id="s-e283d2ed22"></a>`additionalProperties`: `false`
- <a id="s-ab05905591"></a>`description`: `"Success-only, Riverhog-verified result of one resolved join plan."`
- <a id="s-94df2d4913"></a>`required`: `["work_id","workflow_plan_sha256","join_plan_sha256","derivation_sha256","producer_settlement_sha256","output_collection","output_selection","settlement_sha256"]`
- <a id="s-c65afa4ddd"></a>`title`: `"JoinSettlement"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-db3e6acc98"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Derivation Sha256" |  |
| <a id="s-87f6901bd8"></a>`format` | no | type="string"; const="stove0-join-settlement/v1"; default="stove0-join-settlement/v1"; title="Format" |  |
| <a id="s-2846d59ef9"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Join Plan Sha256" |  |
| <a id="s-96a7bf9913"></a>`output_collection` | yes | [CollectionRootIdentityRef](schemas-collectionrootidentityref.md) |  |
| <a id="s-edfa0bba52"></a>`output_selection` | yes | [ArtifactSelectionRef](schemas-artifactselectionref.md) |  |
| <a id="s-c0b09ab379"></a>`producer_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Producer Settlement Sha256" |  |
| <a id="s-022e518efa"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Settlement Sha256" |  |
| <a id="s-79d60be560"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |
| <a id="s-74f0086482"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Workflow Plan Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field derivation_sha256](#s-db3e6acc98) | `length · characters · fixed` | shared above |
| [field join_plan_sha256](#s-2846d59ef9) | `length · characters · fixed` | shared above |
| [field producer_settlement_sha256](#s-c0b09ab379) | `length · characters · fixed` | shared above |
| [field settlement_sha256](#s-022e518efa) | `length · characters · fixed` | shared above |
| [field work_id](#s-79d60be560) | `length · characters · fixed` | shared above |
| [field workflow_plan_sha256](#s-74f0086482) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArtifactSelectionRef](schemas-artifactselectionref.md)
- [CollectionRootIdentityRef](schemas-collectionrootidentityref.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8db1fa2def"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-4223232168"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JoinSettlement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 599f2e5340c643e5c71a0bd9e66eca305eb9cd48088834c2ff9de006bc16851d -->

```json
{
  "additionalProperties": false,
  "description": "Success-only, Riverhog-verified result of one resolved join plan.",
  "properties": {
    "derivation_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Derivation Sha256",
      "type": "string"
    },
    "format": {
      "const": "stove0-join-settlement/v1",
      "default": "stove0-join-settlement/v1",
      "title": "Format",
      "type": "string"
    },
    "join_plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Join Plan Sha256",
      "type": "string"
    },
    "output_collection": {
      "$ref": "#/components/schemas/CollectionRootIdentityRef"
    },
    "output_selection": {
      "$ref": "#/components/schemas/ArtifactSelectionRef"
    },
    "producer_settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Producer Settlement Sha256",
      "type": "string"
    },
    "settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Settlement Sha256",
      "type": "string"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    },
    "workflow_plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Workflow Plan Sha256",
      "type": "string"
    }
  },
  "required": [
    "work_id",
    "workflow_plan_sha256",
    "join_plan_sha256",
    "derivation_sha256",
    "producer_settlement_sha256",
    "output_collection",
    "output_selection",
    "settlement_sha256"
  ],
  "title": "JoinSettlement",
  "type": "object"
}
```

</details>
