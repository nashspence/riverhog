# schemas: ArchiveCopyRetirementTargetOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyretirementtargetout:268b082239 -->

Exact externally visible contract owned by this semantic dossier.

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
| <a id="s-d96da381e5"></a>`last_verified_at` | yes | type="string"; title="Last Verified At" |  |
| <a id="s-ed7d869eec"></a>`object_count` | yes | type="integer"; title="Object Count" |  |
| <a id="s-42f787206b"></a>`remote_storage_bytes` | yes | type="integer"; title="Remote Storage Bytes" |  |
| <a id="s-8fd0830440"></a>`store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |

## Maintained corroboration

### Referenced contract dossiers

- [ArchiveStoreName](schemas-archivestorename.md)

## Governing policies

- <a id="pa-1b43d4e767"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRetirementTargetOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 644fcc1fb33a87da56a4ef230512696f5a547d855e15361bef9f27105e20a11a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "last_verified_at": {
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
