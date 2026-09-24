# schemas: AdmissionView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-admissionview:299ce05a48 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4c588910d9"></a>

- <a id="s-64172b366f"></a>`type`: `"object"`
- <a id="s-bb34f9600d"></a>`additionalProperties`: `false`
- <a id="s-cc8d70a20b"></a>`required`: `["intent","state","attempt_count","created_at","updated_at"]`
- <a id="s-7315e158ba"></a>`title`: `"AdmissionView"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2876199d78"></a>`attempt_count` | yes | type="integer"; minimum=0; title="Attempt Count" |  |
| <a id="s-6df2d3fad6"></a>`created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Created At" |  |
| <a id="s-c08d71353e"></a>`failure` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; title="Failure" |  |
| <a id="s-22ad3d0ba1"></a>`intent` | yes | [AdmissionIntent](schemas-admissionintent.md) |  |
| <a id="s-95bb151c01"></a>`next_attempt_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Next Attempt At" |  |
| <a id="s-c05701d9fd"></a>`preview_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Preview Sha256" |  |
| <a id="s-21f919bc64"></a>`state` | yes | type="string"; enum=["intent","previewed","work_bound"]; title="State" |  |
| <a id="s-500fd66991"></a>`updated_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Updated At" |  |
| <a id="s-93af888f26"></a>`work_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Work Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field created_at](#s-6df2d3fad6) | `length · characters · fixed` | maximum=30; minimum=30; reason="fixed-public-representation" |
| <a id="s-e65943eec0"></a>[field failure · string value](#s-c08d71353e) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-975669e5c3"></a>[field next_attempt_at · string value](#s-95bb151c01) | `length · characters · fixed` | maximum=30; minimum=30; reason="fixed-public-representation" |
| <a id="s-b15f87ebd9"></a>[field preview_sha256 · string value](#s-c05701d9fd) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field updated_at](#s-500fd66991) | `length · characters · fixed` | maximum=30; minimum=30; reason="fixed-public-representation" |
| <a id="s-6df4164e76"></a>[field work_id · string value](#s-93af888f26) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [AdmissionIntent](schemas-admissionintent.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-1749ff8a91"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-cc7174e7c3"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f05b694956d512a0b0836a622c35b33344c4b417203e8d983311a47dea956837 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "attempt_count": {
      "minimum": 0,
      "title": "Attempt Count",
      "type": "integer"
    },
    "created_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Created At",
      "type": "string"
    },
    "failure": {
      "anyOf": [
        {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Failure"
    },
    "intent": {
      "$ref": "#/components/schemas/AdmissionIntent"
    },
    "next_attempt_at": {
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
      "title": "Next Attempt At"
    },
    "preview_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Preview Sha256"
    },
    "state": {
      "enum": [
        "intent",
        "previewed",
        "work_bound"
      ],
      "title": "State",
      "type": "string"
    },
    "updated_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Updated At",
      "type": "string"
    },
    "work_id": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Work Id"
    }
  },
  "required": [
    "intent",
    "state",
    "attempt_count",
    "created_at",
    "updated_at"
  ],
  "title": "AdmissionView",
  "type": "object"
}
```

</details>
