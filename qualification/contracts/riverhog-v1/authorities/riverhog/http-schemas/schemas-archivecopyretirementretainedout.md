# schemas: ArchiveCopyRetirementRetainedOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyretirementretainedout:7ffafbf7b9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d8fb08e288"></a>

- <a id="s-3913756386"></a>`type`: `"object"`
- <a id="s-91ea5d5f7f"></a>`additionalProperties`: `false`
- <a id="s-912f383294"></a>`required`: `["store","last_verified_at","remote_storage_bytes"]`
- <a id="s-c5d98f2579"></a>`title`: `"ArchiveCopyRetirementRetainedOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6548249a87"></a>`last_verified_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Last Verified At" |  |
| <a id="s-ae86cb6aca"></a>`remote_storage_bytes` | yes | type="integer"; title="Remote Storage Bytes" |  |
| <a id="s-a38cb1ae87"></a>`store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field last_verified_at](#s-6548249a87) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-72fb6d99e8"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-83a493746d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRetirementRetainedOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8971d3452741d73b37c78ba89b3784a7e36d9d23f34a45b20251fc0bb378af2d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "last_verified_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Last Verified At",
      "type": "string"
    },
    "remote_storage_bytes": {
      "title": "Remote Storage Bytes",
      "type": "integer"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    }
  },
  "required": [
    "store",
    "last_verified_at",
    "remote_storage_bytes"
  ],
  "title": "ArchiveCopyRetirementRetainedOut",
  "type": "object"
}
```

</details>
