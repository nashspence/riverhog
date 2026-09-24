# schemas: ProcessingOutcomePageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingoutcomepagedocument:61ef9849c1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-6cc8af8e03"></a>

- <a id="s-15301f63ce"></a>`type`: `"object"`
- <a id="s-23542aec68"></a>`additionalProperties`: `false`
- <a id="s-a98900d612"></a>`required`: `["identity","start_ordinal","outcomes"]`
- <a id="s-cf7b99b630"></a>`title`: `"ProcessingOutcomePageDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-01aff9c0d5"></a>`identity` | yes | [ExactSetIdentityDocument](schemas-exactsetidentitydocument.md) |  |
| <a id="s-6f8fcd737f"></a>`next_ordinal` | no | anyOf=[([NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1); (type="null")] |  |
| <a id="s-7532886b30"></a>`outcomes` | yes | type="array"; items=([ProcessingOutcomeIdentityDocument](schemas-processingoutcomeidentitydocument.md)); maxItems=128; title="Outcomes"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"identity-bound-start_ordinal","reason":"bounded-identity-page"} |  |
| <a id="s-4f1f6deba4"></a>`start_ordinal` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: maximum=128; progression={"authority":"processing-claim-outcomes","authority_parameter":"identity_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field outcomes](#s-7532886b30) | `cardinality · items · segmented_no_total_max` | shared above |

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

- [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [ExactSetIdentityDocument](schemas-exactsetidentitydocument.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)
- [ProcessingOutcomeIdentityDocument](schemas-processingoutcomeidentitydocument.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-362574c8ad"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-28789fe45a"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingOutcomePageDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5de1c5de17d07c194f20dd5813de218d5005ba36ee43d790e9c6611d6a0804aa -->

```json
{
  "additionalProperties": false,
  "properties": {
    "identity": {
      "$ref": "#/components/schemas/ExactSetIdentityDocument"
    },
    "next_ordinal": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/NonnegativeDecimal",
          "ge": 1
        },
        {
          "type": "null"
        }
      ]
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
        "progression": "identity-bound-start_ordinal",
        "reason": "bounded-identity-page"
      }
    },
    "start_ordinal": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 0
    }
  },
  "required": [
    "identity",
    "start_ordinal",
    "outcomes"
  ],
  "title": "ProcessingOutcomePageDocument",
  "type": "object"
}
```

</details>
