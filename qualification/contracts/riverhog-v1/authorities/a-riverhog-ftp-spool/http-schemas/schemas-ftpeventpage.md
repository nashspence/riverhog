# schemas: FtpEventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-ftpeventpage:a252609800 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-08c958d4db"></a>

- <a id="s-19e9f8a4f0"></a>`type`: `"object"`
- <a id="s-90ab69b17f"></a>`additionalProperties`: `false`
- <a id="s-b8dbecec0a"></a>`required`: `["events","next_cursor","has_more"]`
- <a id="s-a5827820b4"></a>`title`: `"FtpEventPage"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-61bd33cdfa"></a>`events` | yes | type="array"; items=([FtpLifecycleEvent](schemas-ftplifecycleevent.md)); maxItems=100; title="Events" |  |
| <a id="s-a48b510d66"></a>`has_more` | yes | type="boolean"; title="Has More" |  |
| <a id="s-2b65be0105"></a>`next_cursor` | yes | type="string"; maxLength=220; minLength=1; title="Next Cursor" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: maximum=100; progression={"cursor_parameter":"after","kind":"cursor-feed","limit_parameter":"limit"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field events](#s-61bd33cdfa) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=220; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field next_cursor](#s-2b65be0105) | `length · characters · contract_max` | shared above |

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

- [a-riverhog-ftp-spool-read-collection-progression/v1](../../../evidence/qualifications/a-riverhog-ftp-spool-read-collection-progression-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [FtpLifecycleEvent](schemas-ftplifecycleevent.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-07c9d59a58"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-21c1b87cbe"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-2d13662503"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/FtpEventPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e12b0f05dbb81a7db5d38ec21aa4b7a2b5efa702bc985526accbbac0691a29d4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "events": {
      "items": {
        "$ref": "#/components/schemas/FtpLifecycleEvent"
      },
      "maxItems": 100,
      "title": "Events",
      "type": "array"
    },
    "has_more": {
      "title": "Has More",
      "type": "boolean"
    },
    "next_cursor": {
      "maxLength": 220,
      "minLength": 1,
      "title": "Next Cursor",
      "type": "string"
    }
  },
  "required": [
    "events",
    "next_cursor",
    "has_more"
  ],
  "title": "FtpEventPage",
  "type": "object"
}
```

</details>
