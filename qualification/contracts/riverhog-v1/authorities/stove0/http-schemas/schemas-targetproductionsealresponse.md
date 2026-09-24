# schemas: TargetProductionSealResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-targetproductionsealresponse:6eb66be9d2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-80154241ca"></a>

- <a id="s-8d5d314868"></a>`type`: `"object"`
- <a id="s-a1d74f755e"></a>`additionalProperties`: `false`
- <a id="s-1e5db42538"></a>`required`: `["state"]`
- <a id="s-390c89951d"></a>`title`: `"TargetProductionSealResponse"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b76d61e14c"></a>`production` | no | anyOf=[([TargetProductionAuthority](schemas-targetproductionauthority.md)); (type="null")] |  |
| <a id="s-62b7d0e7eb"></a>`state` | yes | type="string"; enum=["sealing","sealed"]; title="State" |  |

## Maintained corroboration

### Referenced contract elements

- [TargetProductionAuthority](schemas-targetproductionauthority.md)

## Governing policies

- <a id="pa-ffa5d50fb3"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetProductionSealResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 28acdd3e4f69ff52a99c9e90610d9fdf05e9ac3f95c6ca688b864d0bd0e73fe5 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "production": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/TargetProductionAuthority"
        },
        {
          "type": "null"
        }
      ]
    },
    "state": {
      "enum": [
        "sealing",
        "sealed"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "state"
  ],
  "title": "TargetProductionSealResponse",
  "type": "object"
}
```

</details>
