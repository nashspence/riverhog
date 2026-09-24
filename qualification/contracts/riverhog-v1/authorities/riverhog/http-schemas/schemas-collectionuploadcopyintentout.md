# schemas: CollectionUploadCopyIntentOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadcopyintentout:0c8d5213ad -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-645d7b91a1"></a>

- <a id="s-0d9790dc3f"></a>`type`: `"object"`
- <a id="s-9e7247644b"></a>`additionalProperties`: `false`
- <a id="s-1a424d8382"></a>`required`: `["destination_store","state","failure_code","job_state","job_created"]`
- <a id="s-017caf282b"></a>`title`: `"CollectionUploadCopyIntentOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d9f2a208b5"></a>`destination_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-3bf6889842"></a>`failure_code` | yes | anyOf=[(type="string"); (type="null")]; title="Failure Code" |  |
| <a id="s-76389b3144"></a>`job_created` | yes | anyOf=[(type="boolean"); (type="null")]; title="Job Created" |  |
| <a id="s-03a73320a3"></a>`job_state` | yes | anyOf=[(type="string"); (type="null")]; title="Job State" |  |
| <a id="s-2bc667f857"></a>`state` | yes | type="string"; enum=["accepted","pending","handed_off","failed","canceled"]; title="State" |  |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)

## Governing policies

- <a id="pa-55b3ff64ab"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadCopyIntentOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: faf68641fc4848623dca2dcc19bedd85978dcfeb17105d50a489633217957645 -->

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
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Job State"
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
    "job_state",
    "job_created"
  ],
  "title": "CollectionUploadCopyIntentOut",
  "type": "object"
}
```

</details>
