# schemas: ArchiveCopyJobCompletedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyjobcompletedevent:b5961f07e1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-a4a9b8ca39"></a>

- <a id="s-45ecbb2504"></a>`type`: `"object"`
- <a id="s-8efba89221"></a>`additionalProperties`: `false`
- <a id="s-5a2be25bfb"></a>`required`: `["id","type","occurred_at","payload"]`
- <a id="s-872d362028"></a>`title`: `"ArchiveCopyJobCompletedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-db30525c05"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-c0598b1dee"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Occurred At" |  |
| <a id="s-646383a12d"></a>`payload` | yes | [ArchiveCopyJobCompletedData](schemas-archivecopyjobcompleteddata.md) |  |
| <a id="s-b073fe9c25"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; title="Subject" |  |
| <a id="s-c15007550e"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy_job.completed"; title="Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field occurred_at](#s-c0598b1dee) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArchiveCopyJobCompletedData](schemas-archivecopyjobcompleteddata.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-eacfc21023"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-71fd26b8c1"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobCompletedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8159b53d54b5516965492aa46d24d7343e4fc7fdec0d456c709b1b522c47370d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "id": {
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "occurred_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Occurred At",
      "type": "string"
    },
    "payload": {
      "$ref": "#/components/schemas/ArchiveCopyJobCompletedData"
    },
    "subject": {
      "anyOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Subject"
    },
    "type": {
      "const": "io.riverhog.riverhog.archive_copy_job.completed",
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "type",
    "occurred_at",
    "payload"
  ],
  "title": "ArchiveCopyJobCompletedEvent",
  "type": "object"
}
```

</details>
