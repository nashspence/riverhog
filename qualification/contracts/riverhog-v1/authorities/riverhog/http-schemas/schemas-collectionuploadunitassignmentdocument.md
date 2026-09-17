# schemas: CollectionUploadUnitAssignmentDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadunitassignmentdocument:4229dca5d2 -->

One bounded, immutable unit offered by an exact upload session.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-2964a95373"></a>

- <a id="s-2ed2603f50"></a>`type`: `"object"`
- <a id="s-1172987ce2"></a>`additionalProperties`: `false`
- <a id="s-71d774a366"></a>`description`: `"One bounded, immutable unit offered by an exact upload session."`
- <a id="s-9436e19dfd"></a>`required`: `["volume","plan_sha256","unit"]`
- <a id="s-10c77c82e3"></a>`title`: `"CollectionUploadUnitAssignmentDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5c08c11565"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-bac5bfebcc"></a>`unit` | yes | [CollectionUploadUnitWorkDocument](schemas-collectionuploadunitworkdocument.md) |  |
| <a id="s-f21e6e665e"></a>`volume` | yes | [CollectionUploadVolumeSummaryDocument](schemas-collectionuploadvolumesummarydocument.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field plan_sha256](#s-5c08c11565) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionUploadUnitWorkDocument](schemas-collectionuploadunitworkdocument.md)
- [CollectionUploadVolumeSummaryDocument](schemas-collectionuploadvolumesummarydocument.md)

## Governing policies

- <a id="pa-e1494a143d"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-f73958881a"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadUnitAssignmentDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e281174c6520832c75ebed0cb4aa7f8b44365cf8b1745789293a46869d2a60d -->

```json
{
  "additionalProperties": false,
  "description": "One bounded, immutable unit offered by an exact upload session.",
  "properties": {
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "unit": {
      "$ref": "#/components/schemas/CollectionUploadUnitWorkDocument"
    },
    "volume": {
      "$ref": "#/components/schemas/CollectionUploadVolumeSummaryDocument"
    }
  },
  "required": [
    "volume",
    "plan_sha256",
    "unit"
  ],
  "title": "CollectionUploadUnitAssignmentDocument",
  "type": "object"
}
```

</details>
