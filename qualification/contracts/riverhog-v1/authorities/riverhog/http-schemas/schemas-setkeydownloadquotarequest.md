# schemas: SetKeyDownloadQuotaRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-setkeydownloadquotarequest:80b2d25092 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-2b28861351"></a>

- <a id="s-7a74c08ed5"></a>`type`: `"object"`
- <a id="s-a8046636a5"></a>`additionalProperties`: `false`
- <a id="s-de326106bc"></a>`required`: `["monthly_bytes"]`
- <a id="s-cb58ebc063"></a>`title`: `"SetKeyDownloadQuotaRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-60ad711d07"></a>`monthly_bytes` | yes | anyOf=[([MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)); (type="null")] |  |

## Maintained corroboration

### Referenced contract elements

- [MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)

## Governing policies

- <a id="pa-eff3481a49"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/SetKeyDownloadQuotaRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5db7dca3d11b00250fcdfb044e4521283e93acdaaece25d0d55ba8256d0bdd24 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "monthly_bytes": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/MonthlyDownloadQuotaBytes"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "monthly_bytes"
  ],
  "title": "SetKeyDownloadQuotaRequest",
  "type": "object"
}
```

</details>
