# schemas: ContentObservationResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-contentobservationresult:04e724917d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-3f53e49728"></a>

- <a id="s-6e8cb2360a"></a>`type`: `"object"`
- <a id="s-600779f4e3"></a>`additionalProperties`: `false`
- <a id="s-394ad11e3c"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`
- <a id="s-2cfa06f1e2"></a>`title`: `"ContentObservationResult"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d16d5b7097"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Execution Evidence" |  |
| <a id="s-5ffb21851e"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md))); (type="null")]; title="Facts" |  |
| <a id="s-4ebfb2f99d"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](schemas-jsonschemavalidationprofile.md)); (type="null")] |  |
| <a id="s-913fc769d7"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Facts Sha256" |  |
| <a id="s-4100b6fec3"></a>`failure` | no | anyOf=[([ContentObservationFailure](schemas-contentobservationfailure.md)); (type="null")] |  |
| <a id="s-70441662fb"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1"; title="Format" |  |
| <a id="s-07c4f0115c"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](schemas-contentobservationinapplicable.md)); (type="null")] |  |
| <a id="s-90066a9166"></a>`observer` | yes | [ObserverImplementation](schemas-observerimplementation.md) |  |
| <a id="s-be36dfd749"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-a20b4024f7"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-d862e0a7ba"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-ba69908844"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-06c8275ced"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"]; title="State" |  |
| <a id="s-b24d69ecf5"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](schemas-artifactsubject.md)); minItems=1; title="Subjects" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field execution_evidence](#s-d16d5b7097) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-eb1e2facd3"></a>[field facts · object value](#s-5ffb21851e) | `cardinality · entries · operational_policy` | shared above |
| [field subjects](#s-b24d69ecf5) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b5b7b9a805"></a>[field facts_sha256 · string value](#s-913fc769d7) | `length · characters · fixed` | shared above |
| [field observer_contract_sha256](#s-a20b4024f7) | `length · characters · fixed` | shared above |
| [field request_id](#s-d862e0a7ba) | `length · characters · fixed` | shared above |
| [field result_sha256](#s-ba69908844) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArtifactSubject](schemas-artifactsubject.md)
- [ContentObservationFailure](schemas-contentobservationfailure.md)
- [ContentObservationInapplicable](schemas-contentobservationinapplicable.md)
- [JsonSchemaValidationProfile](schemas-jsonschemavalidationprofile.md)
- [JsonValue](schemas-jsonvalue.md)
- [ObserverImplementation](schemas-observerimplementation.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e3aa9c9b2f"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-53b829d6de"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-e54917069e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ContentObservationResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d1005b8acfe5f5af7658e37eb2c3ca494832be2c2b4d8e6010c3a4bd2e1872ab -->

```json
{
  "additionalProperties": false,
  "properties": {
    "execution_evidence": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Execution Evidence",
      "type": "object"
    },
    "facts": {
      "anyOf": [
        {
          "additionalProperties": {
            "$ref": "#/components/schemas/JsonValue"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "title": "Facts"
    },
    "facts_schema": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/JsonSchemaValidationProfile"
        },
        {
          "type": "null"
        }
      ]
    },
    "facts_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Facts Sha256"
    },
    "failure": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ContentObservationFailure"
        },
        {
          "type": "null"
        }
      ]
    },
    "format": {
      "const": "stove0-observation-result/v1",
      "default": "stove0-observation-result/v1",
      "title": "Format",
      "type": "string"
    },
    "inapplicable": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ContentObservationInapplicable"
        },
        {
          "type": "null"
        }
      ]
    },
    "observer": {
      "$ref": "#/components/schemas/ObserverImplementation"
    },
    "observer_contract_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Observer Contract Id",
      "type": "string"
    },
    "observer_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Observer Contract Sha256",
      "type": "string"
    },
    "request_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Request Id",
      "type": "string"
    },
    "result_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Result Sha256",
      "type": "string"
    },
    "state": {
      "enum": [
        "observed",
        "inapplicable",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    },
    "subjects": {
      "items": {
        "$ref": "#/components/schemas/ArtifactSubject"
      },
      "minItems": 1,
      "title": "Subjects",
      "type": "array"
    }
  },
  "required": [
    "request_id",
    "state",
    "observer",
    "observer_contract_id",
    "observer_contract_sha256",
    "subjects",
    "result_sha256"
  ],
  "title": "ContentObservationResult",
  "type": "object"
}
```

</details>
