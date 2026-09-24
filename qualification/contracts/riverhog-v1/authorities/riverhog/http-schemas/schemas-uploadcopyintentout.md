# schemas: UploadCopyIntentOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-uploadcopyintentout:6bc4e9d627 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-098efc50e9"></a>

- <a id="s-a53609c5b1"></a>`type`: `"object"`
- <a id="s-59586c0ad5"></a>`additionalProperties`: `false`
- <a id="s-37b6ee1479"></a>`required`: `["destination_store","state","failure_code","job_created","job_state"]`
- <a id="s-83346603f8"></a>`title`: `"UploadCopyIntentOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5ec0b31d16"></a>`destination_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-ffe17e4bff"></a>`failure_code` | yes | anyOf=[(type="string"); (type="null")]; title="Failure Code" |  |
| <a id="s-d7ff8a6814"></a>`job_created` | yes | anyOf=[(type="boolean"); (type="null")]; title="Job Created" |  |
| <a id="s-066df1e725"></a>`job_state` | yes | anyOf=[([ArchiveCopyJobState](schemas-archivecopyjobstate.md)); (type="null")] |  |
| <a id="s-1c8c4db6bc"></a>`state` | yes | type="string"; enum=["accepted","pending","handed_off","failed","canceled"]; title="State" |  |

## Maintained corroboration

### Referenced contract elements

- [ArchiveCopyJobState](schemas-archivecopyjobstate.md)
- [ArchiveStoreName](schemas-archivestorename.md)

## Governing policies

- <a id="pa-9462513fc5"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/UploadCopyIntentOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c918272aa8ea56434e153311d7c992c71e9ecf177516b05f3c14bbe29ab3ab59 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "destination_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "failure_code": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Failure Code"
    },
    "job_created": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "title": "Job Created"
    },
    "job_state": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArchiveCopyJobState"
        },
        {
          "type": "null"
        }
      ]
    },
    "state": {
      "enum": [
        "accepted",
        "pending",
        "handed_off",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "destination_store",
    "state",
    "failure_code",
    "job_created",
    "job_state"
  ],
  "title": "UploadCopyIntentOut",
  "type": "object"
}
```

</details>
