# schemas: RecipeDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-recipedefinition:035d3387eb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-5d4f1f1b92"></a>

- <a id="s-43a28d388e"></a>`type`: `"object"`
- <a id="s-a1a4bf0364"></a>`additionalProperties`: `false`
- <a id="s-f8a8855661"></a>`required`: `["id","revision","routes","unmatched_artifact_disposition"]`
- <a id="s-dab0766067"></a>`title`: `"RecipeDefinition"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f105baa7b8"></a>`allow_derived_inputs` | no | type="boolean"; default=false; title="Allow Derived Inputs" |  |
| <a id="s-d591b26ad6"></a>`artifact_associations` | no | type="array"; default=[]; items=([ArtifactAssociation](schemas-artifactassociation.md)); title="Artifact Associations" |  |
| <a id="s-57bdc94a63"></a>`event_input_closure` | no | type="string"; const="single-finalized-collection"; default="single-finalized-collection"; title="Event Input Closure" |  |
| <a id="s-f2fcf3466b"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-2211dbf2b8"></a>`join` | no | anyOf=[([RecipeJoin](schemas-recipejoin.md)); (type="null")] |  |
| <a id="s-095b4c1e00"></a>`observers` | no | type="array"; default=[]; items=([ObserverUse](schemas-observeruse.md)); title="Observers" |  |
| <a id="s-8ae96a9025"></a>`revision` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-823b3b9b9f"></a>`routes` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/components/schemas/RecipeCoordinationRoute","operation":"#/components/schemas/RecipeRoute"},"propertyName":"kind"}; oneOf=[([RecipeRoute](schemas-reciperoute.md)); ([RecipeCoordinationRoute](schemas-recipecoordinationroute.md))]); minItems=1; title="Routes" |  |
| <a id="s-b5d61f057b"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0; title="Source Collection Retirement Grace Seconds" |  |
| <a id="s-a6a9990e17"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain"; title="Source Collection Retirement Policy" | Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks. |
| <a id="s-28b7accac5"></a>`unmatched_artifact_disposition` | yes | type="string"; enum=["retain-in-source","reject-work"]; title="Unmatched Artifact Disposition" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_associations](#s-d591b26ad6) | `cardinality · items · operational_policy` | shared above |
| [field observers](#s-095b4c1e00) | `cardinality · items · operational_policy` | shared above |
| [field routes](#s-823b3b9b9f) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArtifactAssociation](schemas-artifactassociation.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)
- [ObserverUse](schemas-observeruse.md)
- [RecipeCoordinationRoute](schemas-recipecoordinationroute.md)
- [RecipeJoin](schemas-recipejoin.md)
- [RecipeRoute](schemas-reciperoute.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-039ec7c752"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-b48f86cd97"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeDefinition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd4423c8ab53df1f4145b646d4cbbebc0499bdebb898b6078d5dd3360a9a8645 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "allow_derived_inputs": {
      "default": false,
      "title": "Allow Derived Inputs",
      "type": "boolean"
    },
    "artifact_associations": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/ArtifactAssociation"
      },
      "title": "Artifact Associations",
      "type": "array"
    },
    "event_input_closure": {
      "const": "single-finalized-collection",
      "default": "single-finalized-collection",
      "title": "Event Input Closure",
      "type": "string"
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "join": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/RecipeJoin"
        },
        {
          "type": "null"
        }
      ]
    },
    "observers": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/ObserverUse"
      },
      "title": "Observers",
      "type": "array"
    },
    "revision": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "routes": {
      "items": {
        "discriminator": {
          "mapping": {
            "coordination": "#/components/schemas/RecipeCoordinationRoute",
            "operation": "#/components/schemas/RecipeRoute"
          },
          "propertyName": "kind"
        },
        "oneOf": [
          {
            "$ref": "#/components/schemas/RecipeRoute"
          },
          {
            "$ref": "#/components/schemas/RecipeCoordinationRoute"
          }
        ]
      },
      "minItems": 1,
      "title": "Routes",
      "type": "array"
    },
    "source_collection_retirement_grace_seconds": {
      "default": 0,
      "minimum": 0,
      "title": "Source Collection Retirement Grace Seconds",
      "type": "integer"
    },
    "source_collection_retirement_policy": {
      "default": "retain",
      "description": "Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks.",
      "enum": [
        "retain",
        "retire-after-verified-output"
      ],
      "title": "Source Collection Retirement Policy",
      "type": "string"
    },
    "unmatched_artifact_disposition": {
      "enum": [
        "retain-in-source",
        "reject-work"
      ],
      "title": "Unmatched Artifact Disposition",
      "type": "string"
    }
  },
  "required": [
    "id",
    "revision",
    "routes",
    "unmatched_artifact_disposition"
  ],
  "title": "RecipeDefinition",
  "type": "object"
}
```

</details>
