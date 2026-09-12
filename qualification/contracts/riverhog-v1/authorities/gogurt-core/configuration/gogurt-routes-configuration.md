# gogurt-routes configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:gogurt-core:gogurt-routes-configuration:a5f533a176 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [configuration](index.md) |
| Family | [documents](index.md#f-88a338520d) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-3d568af478"></a>
- <a id="s-e7ceecdcf1"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed7f6258a4"></a>`kind` | yes | type="string"; const="gogurt.routes" |  |
| <a id="s-eaa36e863e"></a>`routes` | yes | type="object"; additional keys=`additionalProperties`, `propertyNames` |  |
| <a id="s-af0062bb71"></a>`schema_version` | yes | type="integer"; const=1 |  |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"gogurt-routes"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-4024a862be"></a>[field routes · additional values · field command](#s-eaa36e863e) | `cardinality · items · operational_policy` | shared above |
| [field routes](#s-eaa36e863e) | `cardinality · entries · operational_policy` | shared above |

## Governing policies

- <a id="pa-1ddf439332"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-fce524116a"></a>[extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration:gogurt-routes](../../../evidence/sources.md#src-2066f471a7) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/gogurt-routes`

### Exact owned JSON

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
