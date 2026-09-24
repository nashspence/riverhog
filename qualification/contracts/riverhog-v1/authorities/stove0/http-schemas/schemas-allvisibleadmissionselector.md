# schemas: AllVisibleAdmissionSelector

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-allvisibleadmissionselector:eb665eaa38 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-97699ff06b"></a>

- <a id="s-4c097070bc"></a>`type`: `"object"`
- <a id="s-4555347d9d"></a>`additionalProperties`: `false`
- <a id="s-029773f68f"></a>`title`: `"AllVisibleAdmissionSelector"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-15ddcee2be"></a>`kind` | no | type="string"; const="all"; default="all"; title="Kind" |  |

## Governing policies

- <a id="pa-fdc04dcffc"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AllVisibleAdmissionSelector`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 801bb6460b61ee360130fbe0c5b01de8f5d3480455393b40201829dedc673766 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "kind": {
      "const": "all",
      "default": "all",
      "title": "Kind",
      "type": "string"
    }
  },
  "title": "AllVisibleAdmissionSelector",
  "type": "object"
}
```

</details>
