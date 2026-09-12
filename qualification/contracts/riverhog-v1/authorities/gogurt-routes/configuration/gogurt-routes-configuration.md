# gogurt-routes configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:gogurt-routes:gogurt-routes-configuration:24f4f1c1be -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-routes` |
| Interface | `configuration` |
| Family | `documents` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/configuration_documents/gogurt-routes`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configuration-composition/v1`

## Executable sources and proof

- `configuration:gogurt-routes` — `reference/gogurt/packages/core/src/gogurt_core/__init__.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | entries | `operational_policy` | maximum=None, reason=validated-deployment-composition |

## Contract summary

- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `kind` | yes | string |  |
| `routes` | yes | object |  |
| `schema_version` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae7ddc44e19c9813d6eb0f696216b605cec53befe69bcf2a2b45d533d6b6c671 -->

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "kind": {
      "const": "gogurt.routes",
      "type": "string"
    },
    "routes": {
      "additionalProperties": {
        "additionalProperties": false,
        "properties": {
          "command": {
            "items": {
              "minLength": 1,
              "type": "string"
            },
            "minItems": 1,
            "type": "array"
          },
          "enabled": {
            "type": "boolean"
          }
        },
        "required": [
          "command"
        ],
        "type": "object"
      },
      "propertyNames": {
        "pattern": "^[a-z0-9]\u0028?:[a-z0-9-]{0,61}[a-z0-9])?$"
      },
      "type": "object"
    },
    "schema_version": {
      "const": 1,
      "type": "integer"
    }
  },
  "required": [
    "schema_version",
    "kind",
    "routes"
  ],
  "type": "object"
}
```
