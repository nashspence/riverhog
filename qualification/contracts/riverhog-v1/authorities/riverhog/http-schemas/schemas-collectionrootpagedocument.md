# schemas: CollectionRootPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionrootpagedocument:025296e006 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-aba83af95b"></a>

- <a id="s-1c10afcf30"></a>`type`: `"object"`
- <a id="s-b99bbf8f35"></a>`additionalProperties`: `false`
- <a id="s-b5ae72aedc"></a>`required`: `["authority","start_ordinal","inputs"]`
- <a id="s-47becc1df7"></a>`title`: `"CollectionRootPageDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cc3eed5ae7"></a>`authority` | yes | [ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md) |  |
| <a id="s-596a3b3f32"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityDocument](schemas-collectionrootidentitydocument.md)); maxItems=128; title="Inputs"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"authority-bound-start_ordinal","reason":"bounded-authority-page"} |  |
| <a id="s-31aba2331a"></a>`next_ordinal` | no | anyOf=[([NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1); (type="null")] |  |
| <a id="s-b3e9e02376"></a>`start_ordinal` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: maximum=128; progression={"authority":"processing-claim-inputs","authority_parameter":"authority_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field inputs](#s-596a3b3f32) | `cardinality · items · segmented_no_total_max` | shared above |

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

- [CollectionRootIdentityDocument](schemas-collectionrootidentitydocument.md)
- [ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8b351d0741"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-fd9da090ba"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionRootPageDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 261271d97113ecc92a837a013b34882bd853e1a05eae1426cc822e432c705cd4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authority": {
      "$ref": "#/components/schemas/ExactSetAuthorityDocument"
    },
    "inputs": {
      "items": {
        "$ref": "#/components/schemas/CollectionRootIdentityDocument"
      },
      "maxItems": 128,
      "title": "Inputs",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "authority-bound-start_ordinal",
        "reason": "bounded-authority-page"
      }
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
    "authority",
    "start_ordinal",
    "inputs"
  ],
  "title": "CollectionRootPageDocument",
  "type": "object"
}
```

</details>
