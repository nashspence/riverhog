# schemas: TargetProductionSealResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetproductionsealresponse:07b8c9b309 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-80154241ca"></a>
- <a id="s-390c89951d"></a>`title`: TargetProductionSealResponse
- <a id="s-8d5d314868"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b76d61e14c"></a>`production` | no | anyOf=#/components/schemas/TargetProductionAuthority \| type="null" |  |
| <a id="s-62b7d0e7eb"></a>`state` | yes | type="string"; enum=["sealing","sealed"] |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: TargetProductionAuthority](schemas-targetproductionauthority.md)

## Governing policies

- <a id="pa-52e9d55d6f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetProductionSealResponse`

### Exact owned JSON

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
