# schemas: ArchiveCopyRetirementTargetOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyretirementtargetout:268b082239 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d6d3a7e994"></a>

- <a id="s-81d23f5785"></a>`type`: `"object"`
- <a id="s-820f6c04a1"></a>`additionalProperties`: `false`
- <a id="s-3aceff2422"></a>`required`: `["store","last_verified_at","remote_storage_bytes","object_count"]`
- <a id="s-d97136e2e9"></a>`title`: `"ArchiveCopyRetirementTargetOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d96da381e5"></a>`last_verified_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Last Verified At" |  |
| <a id="s-ed7d869eec"></a>`object_count` | yes | type="integer"; title="Object Count" |  |
| <a id="s-42f787206b"></a>`remote_storage_bytes` | yes | type="integer"; title="Remote Storage Bytes" |  |
| <a id="s-8fd0830440"></a>`store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field last_verified_at](#s-d96da381e5) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-1b43d4e767"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-aac4e85dbb"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRetirementTargetOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1bdfc24f7035cb13afe3b7c925bee0e77b942261d704d66cbcd88f9d4b3d40e -->

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
    "object_count": {
      "title": "Object Count",
      "type": "integer"
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
    "remote_storage_bytes",
    "object_count"
  ],
  "title": "ArchiveCopyRetirementTargetOut",
  "type": "object"
}
```

</details>
