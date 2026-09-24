# schemas: HealthOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-healthout:aed4292f37 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-c800f66a5a"></a>

- <a id="s-e1339c581b"></a>`type`: `"object"`
- <a id="s-1112f9763d"></a>`additionalProperties`: `false`
- <a id="s-3acc187d23"></a>`required`: `["service","status"]`
- <a id="s-c01be8718e"></a>`title`: `"HealthOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0860dc5709"></a>`service` | yes | type="string"; minLength=1; title="Service" |  |
| <a id="s-192abe6676"></a>`status` | yes | type="string"; const="ok"; title="Status" |  |

## Governing policies

- <a id="pa-0109b1e053"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/HealthOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea9f1154f04ddb79636546cb2e90459d933f914c58f4a1c0e24a181413556abb -->

```json
{
  "additionalProperties": false,
  "properties": {
    "service": {
      "minLength": 1,
      "title": "Service",
      "type": "string"
    },
    "status": {
      "const": "ok",
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "service",
    "status"
  ],
  "title": "HealthOut",
  "type": "object"
}
```

</details>
