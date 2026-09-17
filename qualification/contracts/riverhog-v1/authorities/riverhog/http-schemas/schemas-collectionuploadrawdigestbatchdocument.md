# schemas: CollectionUploadRawDigestBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadrawdigestbatchdocument:0bb7a6713d -->

One append-only bounded slice of a registered raw source digest sequence.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8c2dbc8625"></a>

- <a id="s-eb89fa257f"></a>`type`: `"object"`
- <a id="s-c809e782aa"></a>`additionalProperties`: `false`
- <a id="s-04494a1222"></a>`description`: `"One append-only bounded slice of a registered raw source digest sequence."`
- <a id="s-71fca8d150"></a>`required`: `["path","first_part","sha256s"]`
- <a id="s-383d2b173e"></a>`title`: `"CollectionUploadRawDigestBatchDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-65e7a28bb3"></a>`first_part` | yes | type="integer"; minimum=0; title="First Part" |  |
| <a id="s-a0efbe2329"></a>`path` | yes | type="string"; title="Path" |  |
| <a id="s-2c06441938"></a>`sha256s` | yes | type="array"; items=(type="string"; pattern="^[0-9a-f]{64}$"); maxItems=1024; minItems=1; title="Sha256S"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"first_part","reason":"bounded-raw-digest-append"} |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=1024; minimum=1; progression={"progression":"first_part"}; reason="bounded-raw-digest-append"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256s](#s-2c06441938) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-ef3601ab55"></a>[field sha256s · items](#s-2c06441938) | `length · characters · fixed` | shared above |

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

- [riverhog-raw-digest-progression/v1](../../../evidence/qualifications/riverhog-raw-digest-progression-v1/index.md)

## Governing policies

- <a id="pa-c61dd9eff6"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-83e7f826e0"></a>[extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)
- <a id="pa-655304e252"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadRawDigestBatchDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b3e9c783bf5afec4ecfc01d05a15564e73cead5a37ad60e657d59c0a4e586f01 -->

```json
{
  "additionalProperties": false,
  "description": "One append-only bounded slice of a registered raw source digest sequence.",
  "properties": {
    "first_part": {
      "minimum": 0,
      "title": "First Part",
      "type": "integer"
    },
    "path": {
      "title": "Path",
      "type": "string"
    },
    "sha256s": {
      "items": {
        "pattern": "^[0-9a-f]{64}$",
        "type": "string"
      },
      "maxItems": 1024,
      "minItems": 1,
      "title": "Sha256S",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "first_part",
        "reason": "bounded-raw-digest-append"
      }
    }
  },
  "required": [
    "path",
    "first_part",
    "sha256s"
  ],
  "title": "CollectionUploadRawDigestBatchDocument",
  "type": "object"
}
```

</details>
