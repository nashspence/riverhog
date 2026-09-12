# schemas: TargetInputPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetinputpage:1ddde286b9 -->

One bounded continuation step through the exact target input authority.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-eef3cd75b1"></a>
- <a id="s-5b44e4512a"></a>`title`: TargetInputPage
- <a id="s-2707e581fa"></a>`description`: One bounded continuation step through the exact target input authority.
- <a id="s-5f57432cbe"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-594372c38b"></a>`artifacts` | yes | type="array"; maxItems=256; items=(#/components/schemas/InputArtifact); additional keys=`x-riverhog-extent` |  |
| <a id="s-e670e38af3"></a>`authority` | yes | #/components/schemas/TargetInputAuthority |  |
| <a id="s-999cc5b16e"></a>`complete` | yes | type="boolean" |  |
| <a id="s-e195a811d3"></a>`continuation` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-e35da8a356"></a>`next_continuation` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: maximum=256; progression={"authority":"target-input-authority","cursor_parameter":"continuation","fixed_limit":256,"kind":"exact-authority-page"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifacts](#s-594372c38b) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-330a239872"></a>[field continuation · string value](#s-e195a811d3) | `length · characters · fixed` | shared above |
| <a id="s-297d81aa68"></a>[field next_continuation · string value](#s-e35da8a356) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: InputArtifact](schemas-inputartifact.md)
- [schemas: TargetInputAuthority](schemas-targetinputauthority.md)

## Governing policies

- <a id="pa-2d2eaee444"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-46c31ee790"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-55c40a09a3"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetInputPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1c887c93d95d18edb5a101a431fd89de17e4147cb72970ca4884f8998ffc070 -->

```json
{
  "additionalProperties": false,
  "description": "One bounded continuation step through the exact target input authority.",
  "properties": {
    "artifacts": {
      "items": {
        "$ref": "#/components/schemas/InputArtifact"
      },
      "maxItems": 256,
      "title": "Artifacts",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "authority-bound-start_ordinal",
        "reason": "bounded-target-input-page"
      }
    },
    "authority": {
      "$ref": "#/components/schemas/TargetInputAuthority"
    },
    "complete": {
      "title": "Complete",
      "type": "boolean"
    },
    "continuation": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Continuation"
    },
    "next_continuation": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Next Continuation"
    }
  },
  "required": [
    "authority",
    "complete",
    "artifacts"
  ],
  "title": "TargetInputPage",
  "type": "object"
}
```
