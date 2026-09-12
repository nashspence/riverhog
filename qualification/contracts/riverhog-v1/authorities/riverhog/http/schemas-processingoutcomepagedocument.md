# schemas: ProcessingOutcomePageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingoutcomepagedocument:44efd07e7b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-6cc8af8e03"></a>
- <a id="s-cf7b99b630"></a>`title`: ProcessingOutcomePageDocument
- <a id="s-15301f63ce"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f0a04d1a20"></a>`authority` | yes | #/components/schemas/ExactSetAuthorityDocument |  |
| <a id="s-6f8fcd737f"></a>`next_ordinal` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |
| <a id="s-7532886b30"></a>`outcomes` | yes | type="array"; maxItems=128; items=(#/components/schemas/ProcessingOutcomeIdentityDocument); additional keys=`x-riverhog-extent` |  |
| <a id="s-4f1f6deba4"></a>`start_ordinal` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: maximum=128; progression={"authority":"processing-claim-outcomes","authority_parameter":"authority_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field outcomes](#s-7532886b30) | `cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)
- [schemas: ProcessingOutcomeIdentityDocument](schemas-processingoutcomeidentitydocument.md)

## Governing policies

- <a id="pa-c19a10d545"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-f57914cdb1"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingOutcomePageDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 54b774bdbd2db22e10c18834277f07064e74c84bd6c884b3bf64b5552f73aaf1 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authority": {
      "$ref": "#/components/schemas/ExactSetAuthorityDocument"
    },
    "next_ordinal": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Next Ordinal"
    },
    "outcomes": {
      "items": {
        "$ref": "#/components/schemas/ProcessingOutcomeIdentityDocument"
      },
      "maxItems": 128,
      "title": "Outcomes",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "authority-bound-start_ordinal",
        "reason": "bounded-authority-page"
      }
    },
    "start_ordinal": {
      "minimum": 0,
      "title": "Start Ordinal",
      "type": "integer"
    }
  },
  "required": [
    "authority",
    "start_ordinal",
    "outcomes"
  ],
  "title": "ProcessingOutcomePageDocument",
  "type": "object"
}
```
