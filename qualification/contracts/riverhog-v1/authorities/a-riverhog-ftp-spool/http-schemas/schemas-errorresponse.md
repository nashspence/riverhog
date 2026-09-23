# schemas: ErrorResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-errorresponse:298a576bd5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-dad3925823"></a>

- <a id="s-f8e3044cbd"></a>`type`: `"object"`
- <a id="s-44d2d60407"></a>`additionalProperties`: `false`
- <a id="s-fc6c05c171"></a>`required`: `["error"]`
- <a id="s-3447a1a3e0"></a>`title`: `"ErrorResponse"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-27d021053c"></a>`error` | yes | [ErrorBody](schemas-errorbody.md) |  |

## Maintained corroboration

### Referenced contract elements

- [ErrorBody](schemas-errorbody.md)

## Governing policies

- <a id="pa-eab9e105b3"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/ErrorResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f512af6aea84106cb490abc5a849a56c6836aec507f2a2906f6d3301ed551137 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "error": {
      "$ref": "#/components/schemas/ErrorBody"
    }
  },
  "required": [
    "error"
  ],
  "title": "ErrorResponse",
  "type": "object"
}
```

</details>
