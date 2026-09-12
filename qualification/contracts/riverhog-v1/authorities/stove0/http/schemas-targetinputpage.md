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

<a id="s-eef3cd75b191"></a>
- <a id="s-5b44e4512a50"></a>`title`: TargetInputPage
- <a id="s-2707e581fac1"></a>`description`: One bounded continuation step through the exact target input authority.
- <a id="s-5f57432cbefa"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-594372c38bb4"></a>`artifacts` | yes | type="array"; maxItems=256; items=(#/components/schemas/InputArtifact); additional keys=`x-riverhog-extent` |  |
| <a id="s-e670e38af309"></a>`authority` | yes | #/components/schemas/TargetInputAuthority |  |
| <a id="s-999cc5b16ef7"></a>`complete` | yes | type="boolean" |  |
| <a id="s-e195a811d3a4"></a>`continuation` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-e35da8a35655"></a>`next_continuation` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)

Shared facts for every subject below: maximum=256; progression={"authority":"target-input-authority","cursor_parameter":"continuation","fixed_limit":256,"kind":"exact-authority-page"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifacts](#s-594372c38bb4) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-330a23987202"></a>field continuation · anyOf alternative 1 | `length · characters · fixed` | shared above |
| <a id="s-297d81aa6838"></a>field next_continuation · anyOf alternative 1 | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: InputArtifact](schemas-inputartifact.md)
- [schemas: TargetInputAuthority](schemas-targetinputauthority.md)

## Governing policies

- <a id="pa-2d2eaee44419"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-46c31ee7903f"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)
- <a id="pa-55c40a09a3f4"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
