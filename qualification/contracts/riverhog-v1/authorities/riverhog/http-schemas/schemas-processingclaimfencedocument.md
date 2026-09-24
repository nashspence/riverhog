# schemas: ProcessingClaimFenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingclaimfencedocument:10027849f1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-2761931570"></a>

- <a id="s-99110fb1b7"></a>`type`: `"object"`
- <a id="s-a53aebbdf6"></a>`additionalProperties`: `false`
- <a id="s-737fa2aa82"></a>`required`: `["fence"]`
- <a id="s-f772f92658"></a>`title`: `"ProcessingClaimFenceDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-399bce7731"></a>`fence` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |

## Maintained corroboration

### Referenced contract elements

- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

- <a id="pa-187f57407e"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimFenceDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 890e8ee67213c0fadd77857f2d4849ad68fe6ea265df4568a8f10679a516c3fe -->

```json
{
  "additionalProperties": false,
  "properties": {
    "fence": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    }
  },
  "required": [
    "fence"
  ],
  "title": "ProcessingClaimFenceDocument",
  "type": "object"
}
```

</details>
