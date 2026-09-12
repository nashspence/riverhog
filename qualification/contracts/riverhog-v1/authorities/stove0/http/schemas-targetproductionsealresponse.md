# schemas: TargetProductionSealResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetproductionsealresponse:07b8c9b309 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: TargetProductionSealResponse
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `production` | no | anyOf=#/components/schemas/TargetProductionAuthority \| type="null" |  |
| `state` | yes | type="string"; enum=["sealing","sealed"] |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: TargetProductionAuthority](schemas-targetproductionauthority.md)

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
