# schemas: ArtifactDispositionOutputPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-artifactdispositionoutputpagedocument:a0cc10fef4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-ed18e124ca"></a>

- <a id="s-d7659f4569"></a>`type`: `"object"`
- <a id="s-48966cfb5b"></a>`additionalProperties`: `false`
- <a id="s-09fd97b6fd"></a>`required`: `["authority","start_ordinal","outputs"]`
- <a id="s-b4be5e79c0"></a>`title`: `"ArtifactDispositionOutputPageDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-38a8152364"></a>`authority` | yes | [ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md) |  |
| <a id="s-ec50ac8374"></a>`next_ordinal` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; title="Next Ordinal" |  |
| <a id="s-e1fedbe99d"></a>`outputs` | yes | type="array"; items=([ArtifactDispositionOutputDocument](schemas-artifactdispositionoutputdocument.md)); maxItems=128; title="Outputs"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"authority-bound-start_ordinal","reason":"bounded-authority-page"} |  |
| <a id="s-3b4b64fa8a"></a>`start_ordinal` | yes | type="integer"; minimum=0; title="Start Ordinal" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: maximum=128; progression={"authority":"processing-claim-disposition-outputs","authority_parameter":"authority_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field outputs](#s-e1fedbe99d) | `cardinality · items · segmented_no_total_max` | shared above |

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

- [ArtifactDispositionOutputDocument](schemas-artifactdispositionoutputdocument.md)
- [ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-709655e9f3"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-f537059ced"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionOutputPageDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7fd3a513c6f8ce999a747fd64b7cf8d921fbccea66aa2295b85c0ed7540d3ac6 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authority": {
      "$ref": "#/components/schemas/ArtifactDispositionSetIdentityDocument"
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
    "outputs": {
      "items": {
        "$ref": "#/components/schemas/ArtifactDispositionOutputDocument"
      },
      "maxItems": 128,
      "title": "Outputs",
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
    "outputs"
  ],
  "title": "ArtifactDispositionOutputPageDocument",
  "type": "object"
}
```

</details>
