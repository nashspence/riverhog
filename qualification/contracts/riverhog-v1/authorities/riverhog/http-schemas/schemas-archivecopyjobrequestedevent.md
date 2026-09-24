# schemas: ArchiveCopyJobRequestedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyjobrequestedevent:c910b1cf89 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4e53477c61"></a>

- <a id="s-650c849516"></a>`type`: `"object"`
- <a id="s-d5b1f5ae50"></a>`additionalProperties`: `false`
- <a id="s-8102fa6789"></a>`required`: `["id","type","occurred_at","payload"]`
- <a id="s-bdd91a5426"></a>`title`: `"ArchiveCopyJobRequestedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0d21d128f5"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-81e2e09118"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Occurred At" |  |
| <a id="s-c8596bbe22"></a>`payload` | yes | [ArchiveCopyJobRequestedData](schemas-archivecopyjobrequesteddata.md) |  |
| <a id="s-6d50fd48ea"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; title="Subject" |  |
| <a id="s-19f7983b5f"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy_job.requested"; title="Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field occurred_at](#s-81e2e09118) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArchiveCopyJobRequestedData](schemas-archivecopyjobrequesteddata.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6df8f6a7d0"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-3d1666bf91"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobRequestedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d080259dd5caaccfaa29a00842e355b3a476910f31905046e1bd31c7ca5a31a5 -->

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
      "$ref": "#/components/schemas/ArchiveCopyJobRequestedData"
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
      "const": "io.riverhog.riverhog.archive_copy_job.requested",
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
  "title": "ArchiveCopyJobRequestedEvent",
  "type": "object"
}
```

</details>
