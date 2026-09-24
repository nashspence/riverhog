# schemas: JoinInputPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-joininputplan:ed5e09f468 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-bd0a2e9964"></a>

- <a id="s-b61d278a28"></a>`type`: `"object"`
- <a id="s-0b7fbddd1d"></a>`additionalProperties`: `false`
- <a id="s-98a85d3ee8"></a>`required`: `["branch_id","settlement_sha256","derivation_sha256","output_collection","artifact_selection"]`
- <a id="s-56ff6e4433"></a>`title`: `"JoinInputPlan"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a54c7517ac"></a>`artifact_selection` | yes | [ArtifactSelectionRef](schemas-artifactselectionref.md) |  |
| <a id="s-fc60225524"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-abe5984f82"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Derivation Sha256" |  |
| <a id="s-1543aa4be8"></a>`output_collection` | yes | [CollectionRootIdentityRef](schemas-collectionrootidentityref.md) |  |
| <a id="s-54a858b5bb"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Producer Settlement Sha256" |  |
| <a id="s-c0b94e9d50"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Settlement Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field derivation_sha256](#s-abe5984f82) | `length · characters · fixed` | shared above |
| <a id="s-7423c3baa0"></a>[field producer_settlement_sha256 · string value](#s-54a858b5bb) | `length · characters · fixed` | shared above |
| [field settlement_sha256](#s-c0b94e9d50) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArtifactSelectionRef](schemas-artifactselectionref.md)
- [CollectionRootIdentityRef](schemas-collectionrootidentityref.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3642398e8c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-4d3ee5c709"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JoinInputPlan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9c61326fa18df90c7371e9fbc8f7737d31bb95cff09fd9aa9910856157faf9ce -->

```json
{
  "additionalProperties": false,
  "properties": {
    "artifact_selection": {
      "$ref": "#/components/schemas/ArtifactSelectionRef"
    },
    "branch_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Branch Id",
      "type": "string"
    },
    "derivation_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Derivation Sha256",
      "type": "string"
    },
    "output_collection": {
      "$ref": "#/components/schemas/CollectionRootIdentityRef"
    },
    "producer_settlement_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Producer Settlement Sha256"
    },
    "settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Settlement Sha256",
      "type": "string"
    }
  },
  "required": [
    "branch_id",
    "settlement_sha256",
    "derivation_sha256",
    "output_collection",
    "artifact_selection"
  ],
  "title": "JoinInputPlan",
  "type": "object"
}
```

</details>
