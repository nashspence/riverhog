# schemas: ObservationResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-observationresult:732d745985 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

<a id="s-362be6f847"></a>
- <a id="s-333a22b624"></a>`title`: ObservationResult
- <a id="s-767426d7f7"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e68ebf9982"></a>`execution_evidence` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-e0e895304b"></a>`facts` | no | anyOf=type="object"; additional keys=`additionalProperties` \| type="null" |  |
| <a id="s-1494511718"></a>`facts_schema` | no | anyOf=#/components/schemas/JsonSchemaDocument \| type="null" |  |
| <a id="s-95b10a81fc"></a>`facts_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-5791d4f0bf"></a>`failure` | no | anyOf=#/components/schemas/ObservationFailure \| type="null" |  |
| <a id="s-6bac73ead1"></a>`format` | no | type="string"; const="stove0-observation-result/v1" |  |
| <a id="s-8f9648eaee"></a>`inapplicable` | no | anyOf=#/components/schemas/ObservationInapplicable \| type="null" |  |
| <a id="s-68bd3b50f5"></a>`observer` | yes | #/components/schemas/ObserverImplementation |  |
| <a id="s-a828b01615"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c8e70d63d0"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-47e38dcff5"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c8d52706f8"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a750e42386"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-1e906affbb"></a>`subjects` | yes | type="array"; minItems=1; items=(#/components/schemas/ArtifactSubject) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field execution_evidence](#s-e68ebf9982) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-1533ebbd27"></a>[field facts · object value](#s-e0e895304b) | `cardinality · entries · operational_policy` | shared above |
| [field subjects](#s-1e906affbb) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-152b2f6588"></a>[field facts_sha256 · string value](#s-95b10a81fc) | `length · characters · fixed` | shared above |
| [field observer_contract_sha256](#s-c8e70d63d0) | `length · characters · fixed` | shared above |
| [field request_id](#s-47e38dcff5) | `length · characters · fixed` | shared above |
| [field result_sha256](#s-c8d52706f8) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactSubject](schemas-artifactsubject.md)
- [schemas: JsonSchemaDocument](schemas-jsonschemadocument.md)
- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: ObservationFailure](schemas-observationfailure.md)
- [schemas: ObservationInapplicable](schemas-observationinapplicable.md)
- [schemas: ObserverImplementation](schemas-observerimplementation.md)

## Governing policies

- <a id="pa-9104f28a0d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-7c5bfffdeb"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-23721bf1c5"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObservationResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f06c7aaea3e4c68bf2da3d3f5ef467b30b67cb3db431233ed43d443146613be -->

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
          "$ref": "#/components/schemas/JsonSchemaDocument"
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
          "$ref": "#/components/schemas/ObservationFailure"
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
          "$ref": "#/components/schemas/ObservationInapplicable"
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
  "title": "ObservationResult",
  "type": "object"
}
```
