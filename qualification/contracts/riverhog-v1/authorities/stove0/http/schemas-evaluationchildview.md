# schemas: EvaluationChildView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationchildview:beddb1d88d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-c87794d161"></a>
- <a id="s-c75964838f"></a>`title`: EvaluationChildView
- <a id="s-d07150de4f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f4f4f554f1"></a>`output` | no | anyOf=#/components/schemas/OutputCollectionRef \| type="null" |  |
| <a id="s-baf274864a"></a>`state` | yes | type="string"; enum=["pending","active","complete","inapplicable","failed","canceled"] |  |
| <a id="s-28b6d88982"></a>`variant_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-794161c858"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field variant_id](#s-28b6d88982) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field work_id](#s-794161c858) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: OutputCollectionRef](schemas-outputcollectionref.md)

## Governing policies

- <a id="pa-e19f6b7128"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-8ecc8e11ae"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationChildView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1092b50b2f3a5e988601a981685280dc7968d0b009e510be37e11e1b87eaab9d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "output": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/OutputCollectionRef"
        },
        {
          "type": "null"
        }
      ]
    },
    "state": {
      "enum": [
        "pending",
        "active",
        "complete",
        "inapplicable",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    },
    "variant_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Variant Id",
      "type": "string"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    }
  },
  "required": [
    "variant_id",
    "work_id",
    "state"
  ],
  "title": "EvaluationChildView",
  "type": "object"
}
```
