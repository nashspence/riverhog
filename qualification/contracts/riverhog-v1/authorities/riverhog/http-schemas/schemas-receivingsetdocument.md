# schemas: ReceivingSetDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-receivingsetdocument:5e958cd83a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f21bf588a0"></a>

- <a id="s-02791136a1"></a>`type`: `"object"`
- <a id="s-c6ce16df0a"></a>`additionalProperties`: `false`
- <a id="s-28f01cf4b5"></a>`required`: `["state","count"]`
- <a id="s-dd60e3fe88"></a>`title`: `"ReceivingSetDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7af5ab6592"></a>`count` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |
| <a id="s-0544752197"></a>`identity` | no | anyOf=[([ExactSetIdentityDocument](schemas-exactsetidentitydocument.md)); (type="null")] |  |
| <a id="s-9d3a07ed65"></a>`state` | yes | type="string"; enum=["receiving","sealed"]; title="State" |  |

## Maintained corroboration

### Referenced contract elements

- [ExactSetIdentityDocument](schemas-exactsetidentitydocument.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

- <a id="pa-0475fe42f5"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ReceivingSetDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eaa0afd45a81414b1bf807b6cf660b88ad5cc8ae6ee81d7e2a3ab1140bdb8ce6 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "count": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 0
    },
    "identity": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ExactSetIdentityDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "state": {
      "enum": [
        "receiving",
        "sealed"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "state",
    "count"
  ],
  "title": "ReceivingSetDocument",
  "type": "object"
}
```

</details>
