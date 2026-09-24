# schemas: ArtifactDispositionOutputBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-artifactdispositionoutputbatchdocument:4a233c3cfe -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-61e1fc1e96"></a>

- <a id="s-e1ee278302"></a>`type`: `"object"`
- <a id="s-cb806a551f"></a>`additionalProperties`: `false`
- <a id="s-c612871ad2"></a>`required`: `["fence","outputs"]`
- <a id="s-808b4bbacc"></a>`title`: `"ArtifactDispositionOutputBatchDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-261db41370"></a>`fence` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-c4cb257ac3"></a>`outputs` | yes | type="array"; items=([ArtifactDispositionOutputDocument](schemas-artifactdispositionoutputdocument.md)); maxItems=128; minItems=1; title="Outputs"; uniqueItems=true; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"sealed-disposition-identity","reason":"bounded-disposition-append"} |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=128; minimum=1; progression={"progression":"sealed-disposition-identity"}; reason="bounded-disposition-append"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field outputs](#s-c4cb257ac3) | `cardinality · items · segmented_no_total_max` | shared above |

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

- [riverhog-work-disposition-append/v1](../../../evidence/qualifications/riverhog-work-disposition-append-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [ArtifactDispositionOutputDocument](schemas-artifactdispositionoutputdocument.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6d22463009"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-acf7dcbf83"></a>[extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionOutputBatchDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 88e78d59caeca07914d9f7c011ef4c7cac04983c9b109ce5b88df8df14a8f170 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "fence": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "outputs": {
      "items": {
        "$ref": "#/components/schemas/ArtifactDispositionOutputDocument"
      },
      "maxItems": 128,
      "minItems": 1,
      "title": "Outputs",
      "type": "array",
      "uniqueItems": true,
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "sealed-disposition-identity",
        "reason": "bounded-disposition-append"
      }
    }
  },
  "required": [
    "fence",
    "outputs"
  ],
  "title": "ArtifactDispositionOutputBatchDocument",
  "type": "object"
}
```

</details>
