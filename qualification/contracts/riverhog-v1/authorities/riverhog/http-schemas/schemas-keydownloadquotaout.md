# schemas: KeyDownloadQuotaOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-keydownloadquotaout:37fb917b61 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-c607c0529c"></a>

- <a id="s-5c422e1a11"></a>`type`: `"object"`
- <a id="s-ddd210c17c"></a>`additionalProperties`: `false`
- <a id="s-35f09b222e"></a>`required`: `["id","app","key_id","key_status","monthly_bytes","month_started_at","resets_at","accounted_bytes","reserved_bytes","remaining_bytes"]`
- <a id="s-b7f4c7bed0"></a>`title`: `"KeyDownloadQuotaOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0f6dcdca4d"></a>`accounted_bytes` | yes | type="integer"; minimum=0; title="Accounted Bytes" |  |
| <a id="s-594d83539c"></a>`app` | yes | [ApplicationName](schemas-applicationname.md) |  |
| <a id="s-bf55e128d8"></a>`id` | yes | type="string"; title="Id" |  |
| <a id="s-78846b1bfc"></a>`key_id` | yes | [ApplicationKeyId](schemas-applicationkeyid.md) |  |
| <a id="s-a6a9ffecad"></a>`key_status` | yes | type="string"; enum=["active","expired","revoked"]; title="Key Status" |  |
| <a id="s-a5c3111220"></a>`month_started_at` | yes | type="string"; title="Month Started At" |  |
| <a id="s-b33af12726"></a>`monthly_bytes` | yes | anyOf=[([MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)); (type="null")] |  |
| <a id="s-06aaaaa45e"></a>`remaining_bytes` | yes | anyOf=[(type="integer"; minimum=0); (type="null")]; title="Remaining Bytes" |  |
| <a id="s-60638ee712"></a>`reserved_bytes` | yes | type="integer"; minimum=0; title="Reserved Bytes" |  |
| <a id="s-76ad698142"></a>`resets_at` | yes | type="string"; title="Resets At" |  |

## Maintained corroboration

### Referenced contract elements

- [ApplicationKeyId](schemas-applicationkeyid.md)
- [ApplicationName](schemas-applicationname.md)
- [MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)

## Governing policies

- <a id="pa-a502055e67"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/KeyDownloadQuotaOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61974ed36a10ed767b51cd2ba12717626346883603fb66eb7d4c249484173525 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "accounted_bytes": {
      "minimum": 0,
      "title": "Accounted Bytes",
      "type": "integer"
    },
    "app": {
      "$ref": "#/components/schemas/ApplicationName"
    },
    "id": {
      "title": "Id",
      "type": "string"
    },
    "key_id": {
      "$ref": "#/components/schemas/ApplicationKeyId"
    },
    "key_status": {
      "enum": [
        "active",
        "expired",
        "revoked"
      ],
      "title": "Key Status",
      "type": "string"
    },
    "month_started_at": {
      "title": "Month Started At",
      "type": "string"
    },
    "monthly_bytes": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/MonthlyDownloadQuotaBytes"
        },
        {
          "type": "null"
        }
      ]
    },
    "remaining_bytes": {
      "anyOf": [
        {
          "minimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Remaining Bytes"
    },
    "reserved_bytes": {
      "minimum": 0,
      "title": "Reserved Bytes",
      "type": "integer"
    },
    "resets_at": {
      "title": "Resets At",
      "type": "string"
    }
  },
  "required": [
    "id",
    "app",
    "key_id",
    "key_status",
    "monthly_bytes",
    "month_started_at",
    "resets_at",
    "accounted_bytes",
    "reserved_bytes",
    "remaining_bytes"
  ],
  "title": "KeyDownloadQuotaOut",
  "type": "object"
}
```

</details>
