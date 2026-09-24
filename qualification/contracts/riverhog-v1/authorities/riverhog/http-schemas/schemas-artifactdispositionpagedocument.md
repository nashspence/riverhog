# schemas: ArtifactDispositionPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-artifactdispositionpagedocument:f351d0938c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-b33c27141e"></a>

- <a id="s-aaa0bbc906"></a>`type`: `"object"`
- <a id="s-358cba2955"></a>`additionalProperties`: `false`
- <a id="s-c567ae74f3"></a>`required`: `["identity","start_ordinal","dispositions"]`
- <a id="s-6f14e0333f"></a>`title`: `"ArtifactDispositionPageDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-300c0e231e"></a>`dispositions` | yes | type="array"; items=([ArtifactDispositionDocument](schemas-artifactdispositiondocument.md)); maxItems=128; title="Dispositions"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"identity-bound-start_ordinal","reason":"bounded-identity-page"} |  |
| <a id="s-c91ecab215"></a>`identity` | yes | [ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md) |  |
| <a id="s-964eecf671"></a>`next_ordinal` | no | anyOf=[([NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1); (type="null")] |  |
| <a id="s-986071d04a"></a>`start_ordinal` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: maximum=128; progression={"authority":"processing-claim-dispositions","authority_parameter":"identity_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field dispositions](#s-300c0e231e) | `cardinality · items · segmented_no_total_max` | shared above |

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

- [ArtifactDispositionDocument](schemas-artifactdispositiondocument.md)
- [ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-da6963f609"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-adbd2e855c"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionPageDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98ef36024bb9015e2a604cee3618ae45f8d58c33e7fa837efd6e6439c1feef77 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "dispositions": {
      "items": {
        "$ref": "#/components/schemas/ArtifactDispositionDocument"
      },
      "maxItems": 128,
      "title": "Dispositions",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "identity-bound-start_ordinal",
        "reason": "bounded-identity-page"
      }
    },
    "identity": {
      "$ref": "#/components/schemas/ArtifactDispositionSetIdentityDocument"
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
    "start_ordinal": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 0
    }
  },
  "required": [
    "identity",
    "start_ordinal",
    "dispositions"
  ],
  "title": "ArtifactDispositionPageDocument",
  "type": "object"
}
```

</details>
