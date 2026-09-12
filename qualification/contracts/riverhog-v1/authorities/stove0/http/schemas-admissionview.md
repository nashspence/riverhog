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

<a id="s-4c588910d987"></a>
- <a id="s-7315e158ba38"></a>`title`: AdmissionView
- <a id="s-64172b366fe1"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2876199d7870"></a>`attempt_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-6df2d3fad6d2"></a>`created_at` | yes | type="string"; minLength=1; maxLength=40 |  |
| <a id="s-c08d71353e10"></a>`failure` | no | anyOf=type="string"; minLength=1; maxLength=1000 \| type="null" |  |
| <a id="s-22ad3d0ba160"></a>`intent` | yes | #/components/schemas/AdmissionIntent |  |
| <a id="s-95bb151c013a"></a>`next_attempt_at` | no | anyOf=type="string"; minLength=1; maxLength=40 \| type="null" |  |
| <a id="s-c05701d9fd74"></a>`preview_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-21f919bc647c"></a>`state` | yes | type="string"; enum=["intent","previewed","work_bound"] |  |
| <a id="s-500fd6699179"></a>`updated_at` | yes | type="string"; minLength=1; maxLength=40 |  |
| <a id="s-93af888f26ac"></a>`work_id` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field created_at](#s-6df2d3fad6d2) | `length · characters · contract_max` | maximum=40; minimum=1; reason="schema-maximum" |
| <a id="s-e65943eec064"></a>field failure · anyOf alternative 1 | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-975669e5c368"></a>field next_attempt_at · anyOf alternative 1 | `length · characters · contract_max` | maximum=40; minimum=1; reason="schema-maximum" |
| <a id="s-b15f87ebd99e"></a>field preview_sha256 · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field updated_at](#s-500fd6699179) | `length · characters · contract_max` | maximum=40; minimum=1; reason="schema-maximum" |
| <a id="s-6df4164e7682"></a>field work_id · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: AdmissionIntent](schemas-admissionintent.md)

## Governing policies

- <a id="pa-f4ea538aeb36"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-0e4e24baef19"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
