# schemas: AdmissionView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionview:e44d6c6b0c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 6 |

## External contract

<a id="s-4c588910d9"></a>
- <a id="s-7315e158ba"></a>`title`: AdmissionView
- <a id="s-64172b366f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2876199d78"></a>`attempt_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-6df2d3fad6"></a>`created_at` | yes | type="string"; minLength=1; maxLength=40 |  |
| <a id="s-c08d71353e"></a>`failure` | no | anyOf=type="string"; minLength=1; maxLength=1000 \| type="null" |  |
| <a id="s-22ad3d0ba1"></a>`intent` | yes | #/components/schemas/AdmissionIntent |  |
| <a id="s-95bb151c01"></a>`next_attempt_at` | no | anyOf=type="string"; minLength=1; maxLength=40 \| type="null" |  |
| <a id="s-c05701d9fd"></a>`preview_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-21f919bc64"></a>`state` | yes | type="string"; enum=["intent","previewed","work_bound"] |  |
| <a id="s-500fd66991"></a>`updated_at` | yes | type="string"; minLength=1; maxLength=40 |  |
| <a id="s-93af888f26"></a>`work_id` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field created_at](#s-6df2d3fad6) | `length · characters · contract_max` | maximum=40; minimum=1; reason="schema-maximum" |
| <a id="s-e65943eec0"></a>[field failure · string value](#s-c08d71353e) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-975669e5c3"></a>[field next_attempt_at · string value](#s-95bb151c01) | `length · characters · contract_max` | maximum=40; minimum=1; reason="schema-maximum" |
| <a id="s-b15f87ebd9"></a>[field preview_sha256 · string value](#s-c05701d9fd) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field updated_at](#s-500fd66991) | `length · characters · contract_max` | maximum=40; minimum=1; reason="schema-maximum" |
| <a id="s-6df4164e76"></a>[field work_id · string value](#s-93af888f26) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: AdmissionIntent](schemas-admissionintent.md)

## Governing policies

- <a id="pa-f4ea538aeb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-0e4e24baef"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cba7e1704e5373153f3f9f290e884c6d67733a32baed9f56bec9097eb280ffe -->

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
      "maxLength": 40,
      "minLength": 1,
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
          "maxLength": 40,
          "minLength": 1,
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
      "maxLength": 40,
      "minLength": 1,
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
