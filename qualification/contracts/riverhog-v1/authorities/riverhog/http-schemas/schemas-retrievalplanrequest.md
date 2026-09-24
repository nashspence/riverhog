# schemas: RetrievalPlanRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalplanrequest:7b28933b22 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4b2e57f624"></a>

- <a id="s-4a8dc3ad10"></a>`type`: `"object"`
- <a id="s-5c5041a734"></a>`additionalProperties`: `false`
- <a id="s-b58681aa7d"></a>`required`: `["files","idempotency_key"]`
- <a id="s-8aa7907ec0"></a>`title`: `"RetrievalPlanRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-92156c5164"></a>`files` | yes | type="array"; items=([RetrievalFileReferenceDocument](schemas-retrievalfilereferencedocument.md)); maxItems=10000; minItems=1; title="Files"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"multiple-retrieval-jobs","reason":"bounded-retrieval-work-request"} |  |
| <a id="s-1281abc044"></a>`idempotency_key` | yes | type="string"; maxLength=200; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$"; title="Idempotency Key" |  |
| <a id="s-ca9836474f"></a>`lease_seconds` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; title="Lease Seconds" |  |
| <a id="s-f33de244d2"></a>`restore_policy` | no | type="string"; enum=["allow","never"]; default="allow"; title="Restore Policy" |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=10000; minimum=1; progression={"progression":"multiple-retrieval-jobs"}; reason="bounded-retrieval-work-request"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-92156c5164) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=200; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field idempotency_key](#s-1281abc044) | `length · characters · contract_max` | shared above |

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

- [riverhog-retrieval-work-progression/v1](../../../evidence/qualifications/riverhog-retrieval-work-progression-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [RetrievalFileReferenceDocument](schemas-retrievalfilereferencedocument.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b9024a3331"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-389764c3ef"></a>[extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)
- <a id="pa-a864915f4e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d0713d37a8808e79acb34c07987edcd1d422626671a7dcf49e3a452cbbaf30d0 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "files": {
      "items": {
        "$ref": "#/components/schemas/RetrievalFileReferenceDocument"
      },
      "maxItems": 10000,
      "minItems": 1,
      "title": "Files",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "multiple-retrieval-jobs",
        "reason": "bounded-retrieval-work-request"
      }
    },
    "idempotency_key": {
      "maxLength": 200,
      "minLength": 1,
      "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
      "title": "Idempotency Key",
      "type": "string"
    },
    "lease_seconds": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Lease Seconds"
    },
    "restore_policy": {
      "default": "allow",
      "enum": [
        "allow",
        "never"
      ],
      "title": "Restore Policy",
      "type": "string"
    }
  },
  "required": [
    "files",
    "idempotency_key"
  ],
  "title": "RetrievalPlanRequest",
  "type": "object"
}
```

</details>
