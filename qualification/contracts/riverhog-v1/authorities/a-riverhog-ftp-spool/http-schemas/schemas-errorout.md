# schemas: ErrorOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-errorout:d2fd3c866b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8b1290ff06"></a>

- <a id="s-e43faf4ff1"></a>`type`: `"object"`
- <a id="s-ff703ce3a9"></a>`additionalProperties`: `false`
- <a id="s-0d28d4620f"></a>`required`: `["error"]`
- <a id="s-0019174c9b"></a>`title`: `"ErrorOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-02a8af268b"></a>`error` | yes | [ErrorBody](schemas-errorbody.md) |  |

## Maintained corroboration

### Referenced contract elements

- [ErrorBody](schemas-errorbody.md)

## Governing policies

- <a id="pa-809d40f37f"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/ErrorOut`

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
