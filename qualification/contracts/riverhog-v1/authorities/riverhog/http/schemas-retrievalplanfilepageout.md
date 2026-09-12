# schemas: RetrievalPlanFilePageOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalplanfilepageout:d5463153ff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-35d2ab1520"></a>
- <a id="s-9d3361e816"></a>`title`: RetrievalPlanFilePageOut
- <a id="s-ed412b4683"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-328cd49682"></a>`complete` | yes | type="boolean" |  |
| <a id="s-02762d6e31"></a>`etag` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-20cf25d13a"></a>`files` | yes | type="array"; maxItems=100; items=(#/components/schemas/RetrievalPlanFileOut) |  |
| <a id="s-3f4953179c"></a>`format` | yes | type="string"; const="riverhog-retrieval-plan-files/v1" |  |
| <a id="s-b41186f25a"></a>`next_ordinal` | no | anyOf=type="integer"; minimum=1; maximum=10000 \| type="null" |  |
| <a id="s-a644293808"></a>`plan_id` | yes | type="string" |  |
| <a id="s-8dc3c38e1e"></a>`start_ordinal` | yes | type="integer"; minimum=0; maximum=10000 |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: maximum=100; progression={"authority":"retrieval-plan-files","cursor_parameter":"start_ordinal","kind":"exact-authority-page","limit_parameter":"page_size"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-20cf25d13a) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field etag](#s-02762d6e31) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-081802f705"></a>[field next_ordinal · integer value](#s-b41186f25a) | `value · schema-value · contract_max` | maximum=10000; minimum=1; reason="schema-maximum" |
| [field start_ordinal](#s-8dc3c38e1e) | `value · schema-value · contract_max` | maximum=10000; minimum=0; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: RetrievalPlanFileOut](schemas-retrievalplanfileout.md)

## Governing policies

- <a id="pa-d2f7b2f73c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-c318fe9437"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-ccc3a56c4c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanFilePageOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bef4038a36e91bbf83a288398920aeeb633f4507becfdc7557f1fcb611608872 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "complete": {
      "title": "Complete",
      "type": "boolean"
    },
    "etag": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Etag",
      "type": "string"
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/RetrievalPlanFileOut"
      },
      "maxItems": 100,
      "title": "Files",
      "type": "array"
    },
    "format": {
      "const": "riverhog-retrieval-plan-files/v1",
      "title": "Format",
      "type": "string"
    },
    "next_ordinal": {
      "anyOf": [
        {
          "maximum": 10000,
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Next Ordinal"
    },
    "plan_id": {
      "title": "Plan Id",
      "type": "string"
    },
    "start_ordinal": {
      "maximum": 10000,
      "minimum": 0,
      "title": "Start Ordinal",
      "type": "integer"
    }
  },
  "required": [
    "format",
    "plan_id",
    "etag",
    "start_ordinal",
    "complete",
    "files"
  ],
  "title": "RetrievalPlanFilePageOut",
  "type": "object"
}
```
