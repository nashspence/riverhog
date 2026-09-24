# schemas: ArtifactSelectionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-artifactselectionpage:f127970292 -->

One bounded continuation step through an immutable artifact selection.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-1f953a503d"></a>

- <a id="s-09d033a673"></a>`type`: `"object"`
- <a id="s-47e3d02aba"></a>`additionalProperties`: `false`
- <a id="s-e4d5c0e676"></a>`description`: `"One bounded continuation step through an immutable artifact selection."`
- <a id="s-daf13e6b59"></a>`required`: `["authority","complete","artifacts"]`
- <a id="s-6e688cb964"></a>`title`: `"ArtifactSelectionPage"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4fcf66d85f"></a>`artifacts` | yes | type="array"; items=([WorkArtifactSubject](schemas-workartifactsubject.md)); maxItems=256; title="Artifacts"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"selection-bound-start_ordinal","reason":"bounded-artifact-selection-page"} |  |
| <a id="s-bb912bc81c"></a>`authority` | yes | [ArtifactSelectionRef](schemas-artifactselectionref.md) |  |
| <a id="s-53df9f7d4c"></a>`complete` | yes | type="boolean"; title="Complete" |  |
| <a id="s-dbf38f6d33"></a>`continuation` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Continuation" |  |
| <a id="s-b2798753b1"></a>`next_continuation` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Next Continuation" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: maximum=256; progression={"authority":"artifact-selection","authority_parameter":"selection_sha256","cursor_parameter":"continuation","fixed_limit":256,"kind":"exact-authority-page"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifacts](#s-4fcf66d85f) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-96cb858d39"></a>[field continuation · string value](#s-dbf38f6d33) | `length · characters · fixed` | shared above |
| <a id="s-cd4fa13c3b"></a>[field next_continuation · string value](#s-b2798753b1) | `length · characters · fixed` | shared above |

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

- [ArtifactSelectionRef](schemas-artifactselectionref.md)
- [WorkArtifactSubject](schemas-workartifactsubject.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-91c712fd92"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-2cf9b11648"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-c2eef5ed57"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactSelectionPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: feb69833f39fa8b8052a818786703d1530a7a37202d464899950d3e22e406e94 -->

```json
{
  "additionalProperties": false,
  "description": "One bounded continuation step through an immutable artifact selection.",
  "properties": {
    "artifacts": {
      "items": {
        "$ref": "#/components/schemas/WorkArtifactSubject"
      },
      "maxItems": 256,
      "title": "Artifacts",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "selection-bound-start_ordinal",
        "reason": "bounded-artifact-selection-page"
      }
    },
    "authority": {
      "$ref": "#/components/schemas/ArtifactSelectionRef"
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
  "title": "ArtifactSelectionPage",
  "type": "object"
}
```

</details>
