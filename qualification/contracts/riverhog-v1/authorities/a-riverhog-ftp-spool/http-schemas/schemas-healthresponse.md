# schemas: HealthResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-healthresponse:3135de24ed -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-fdd93529c0"></a>

- <a id="s-16ea5f9db4"></a>`type`: `"object"`
- <a id="s-3a418e9e23"></a>`additionalProperties`: `false`
- <a id="s-db92fd0d12"></a>`required`: `["service","status"]`
- <a id="s-94d5eacf6e"></a>`title`: `"HealthResponse"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-73aec2b6c1"></a>`service` | yes | type="string"; minLength=1; title="Service" |  |
| <a id="s-965042841b"></a>`status` | yes | type="string"; const="ok"; title="Status" |  |

## Governing policies

- <a id="pa-f7bd67bebe"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/HealthResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 873f58b65973a85d82bd4e352acd595a8f32f6058c4500f11514358669b42b31 -->

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
  "title": "HealthResponse",
  "type": "object"
}
```

</details>
