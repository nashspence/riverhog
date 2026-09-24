# schemas: CollectionProvenanceVerificationJobOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionprovenanceverificationjobout:5459a07e99 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-968608cbbf"></a>

- <a id="s-0e00b9ee87"></a>`type`: `"object"`
- <a id="s-cdc338302b"></a>`additionalProperties`: `false`
- <a id="s-442f0f444b"></a>`required`: `["collection_id","state","requested_at","started_at","finished_at","attempts","result","failure"]`
- <a id="s-c514fe5c30"></a>`title`: `"CollectionProvenanceVerificationJobOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-47daac55a0"></a>`attempts` | yes | type="integer"; minimum=0; title="Attempts" |  |
| <a id="s-6ca57b3926"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-b6bb0512f6"></a>`failure` | yes | anyOf=[(type="string"); (type="null")]; title="Failure" |  |
| <a id="s-c7a8ebe491"></a>`finished_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Finished At" |  |
| <a id="s-f484534cb7"></a>`requested_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Requested At" |  |
| <a id="s-de3dc86efe"></a>`result` | yes | anyOf=[([CollectionProvenanceVerificationOut](schemas-collectionprovenanceverificationout.md)); (type="null")] |  |
| <a id="s-e54a5477bb"></a>`started_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Started At" |  |
| <a id="s-32e51516fe"></a>`state` | yes | type="string"; enum=["queued","running","canceling","succeeded","failed","canceled"]; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-e66492f752"></a>[field finished_at · string value](#s-c7a8ebe491) | `length · characters · fixed` | shared above |
| [field requested_at](#s-f484534cb7) | `length · characters · fixed` | shared above |
| <a id="s-6c471ba2c8"></a>[field started_at · string value](#s-e54a5477bb) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)
- [CollectionProvenanceVerificationOut](schemas-collectionprovenanceverificationout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-7f92ebf17b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-3b85effb90"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionProvenanceVerificationJobOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 76fc929b6c5b95ad22357a1e87da9da557603845414817a280e7d3cd87d9f5c2 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "attempts": {
      "minimum": 0,
      "title": "Attempts",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "failure": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Failure"
    },
    "finished_at": {
      "anyOf": [
        {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Finished At"
    },
    "requested_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Requested At",
      "type": "string"
    },
    "result": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionProvenanceVerificationOut"
        },
        {
          "type": "null"
        }
      ]
    },
    "started_at": {
      "anyOf": [
        {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Started At"
    },
    "state": {
      "enum": [
        "queued",
        "running",
        "canceling",
        "succeeded",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "state",
    "requested_at",
    "started_at",
    "finished_at",
    "attempts",
    "result",
    "failure"
  ],
  "title": "CollectionProvenanceVerificationJobOut",
  "type": "object"
}
```

</details>
