# schemas: TargetInputPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-targetinputpage:f26955ebf6 -->

One bounded continuation step through the exact target input authority.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-eef3cd75b1"></a>

- <a id="s-5f57432cbe"></a>`type`: `"object"`
- <a id="s-a749878f2a"></a>`additionalProperties`: `false`
- <a id="s-2707e581fa"></a>`description`: `"One bounded continuation step through the exact target input authority."`
- <a id="s-a21ebd4afe"></a>`required`: `["authority","complete","artifacts"]`
- <a id="s-5b44e4512a"></a>`title`: `"TargetInputPage"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-594372c38b"></a>`artifacts` | yes | type="array"; items=([InputArtifact](schemas-inputartifact.md)); maxItems=256; title="Artifacts"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"authority-bound-start_ordinal","reason":"bounded-target-input-page"} |  |
| <a id="s-e670e38af3"></a>`authority` | yes | [TargetInputAuthority](schemas-targetinputauthority.md) |  |
| <a id="s-999cc5b16e"></a>`complete` | yes | type="boolean"; title="Complete" |  |
| <a id="s-e195a811d3"></a>`continuation` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Continuation" |  |
| <a id="s-e35da8a356"></a>`next_continuation` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Next Continuation" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: maximum=256; progression={"authority":"target-input-authority","cursor_parameter":"continuation","fixed_limit":256,"kind":"exact-authority-page"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifacts](#s-594372c38b) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-330a239872"></a>[field continuation · string value](#s-e195a811d3) | `length · characters · fixed` | shared above |
| <a id="s-297d81aa68"></a>[field next_continuation · string value](#s-e35da8a356) | `length · characters · fixed` | shared above |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb).

Exact evidence groups for this contract element:

- [stove0-read-collection-progression/v1](../../../evidence/qualifications/stove0-read-collection-progression-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [InputArtifact](schemas-inputartifact.md)
- [TargetInputAuthority](schemas-targetinputauthority.md)

## Governing policies

- <a id="pa-410b0ad895"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-dc2ce2bb91"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-3f7d0e1ff6"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetInputPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
