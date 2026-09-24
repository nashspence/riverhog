# schemas: ErrorOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-errorout:d500f7a1f9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-7b12dd6800"></a>

- <a id="s-330851d63f"></a>`type`: `"object"`
- <a id="s-a48ae87af5"></a>`additionalProperties`: `false`
- <a id="s-8e9f5cd7d1"></a>`required`: `["error"]`
- <a id="s-1e888a6770"></a>`title`: `"ErrorOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6e0a721df6"></a>`error` | yes | [ErrorBody](schemas-errorbody.md) |  |

## Maintained corroboration

### Referenced contract elements

- [ErrorBody](schemas-errorbody.md)

## Governing policies

- <a id="pa-d15f790273"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ErrorOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 68fccceb4b630d83ab04e0dae1490c6d00f6d8746db0fe72b820d1f2c6dbad62 -->

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
  "title": "ErrorOut",
  "type": "object"
}
```

</details>
