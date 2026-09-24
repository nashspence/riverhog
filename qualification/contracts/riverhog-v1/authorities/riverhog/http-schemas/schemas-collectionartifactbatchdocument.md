# schemas: CollectionArtifactBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionartifactbatchdocument:4c05eb68b3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-5d67106b76"></a>

- <a id="s-97b4185eff"></a>`type`: `"object"`
- <a id="s-39358bc346"></a>`additionalProperties`: `false`
- <a id="s-9dab28630e"></a>`required`: `["fence","start_ordinal","artifacts"]`
- <a id="s-b4f3aea380"></a>`title`: `"CollectionArtifactBatchDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1c0906aa0f"></a>`artifacts` | yes | type="array"; items=([CollectionArtifactIdentityDocument](schemas-collectionartifactidentitydocument.md)); maxItems=128; minItems=1; title="Artifacts"; uniqueItems=true; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"start_ordinal","reason":"bounded-set-append"} |  |
| <a id="s-1c82d23b05"></a>`fence` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-7ef1e1094b"></a>`start_ordinal` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=128; minimum=1; progression={"progression":"start_ordinal"}; reason="bounded-set-append"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifacts](#s-1c0906aa0f) | `cardinality · items · segmented_no_total_max` | shared above |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594).

Exact evidence groups for this contract element:

- [riverhog-work-set-append/v1](../../../evidence/qualifications/riverhog-work-set-append-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [CollectionArtifactIdentityDocument](schemas-collectionartifactidentitydocument.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-bc2a963eda"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-692d781553"></a>[extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionArtifactBatchDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d4275f21ad04cb5f7e16f9ef880b1417dbd10ab6301f65c713c21c6e53ac4380 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "artifacts": {
      "items": {
        "$ref": "#/components/schemas/CollectionArtifactIdentityDocument"
      },
      "maxItems": 128,
      "minItems": 1,
      "title": "Artifacts",
      "type": "array",
      "uniqueItems": true,
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "start_ordinal",
        "reason": "bounded-set-append"
      }
    },
    "fence": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "start_ordinal": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 0
    }
  },
  "required": [
    "fence",
    "start_ordinal",
    "artifacts"
  ],
  "title": "CollectionArtifactBatchDocument",
  "type": "object"
}
```

</details>
