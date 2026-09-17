# schemas: HealthResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-healthresponse:9b5462cad2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-496ae350eb"></a>

- <a id="s-dc701a6438"></a>`type`: `"object"`
- <a id="s-ee58ca53c9"></a>`additionalProperties`: `false`
- <a id="s-ec64683468"></a>`required`: `["service","status"]`
- <a id="s-1afae284d8"></a>`title`: `"HealthResponse"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ef758b3f6a"></a>`service` | yes | type="string"; minLength=1; title="Service" |  |
| <a id="s-8587ad7031"></a>`status` | yes | type="string"; const="ok"; title="Status" |  |

## Governing policies

- <a id="pa-11beee3d5c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/HealthResponse`

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
