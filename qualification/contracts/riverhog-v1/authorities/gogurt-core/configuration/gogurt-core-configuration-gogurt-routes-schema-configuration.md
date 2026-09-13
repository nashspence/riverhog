# gogurt-core:configuration:gogurt-routes-schema configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:gogurt-core:gogurt-core-configuration-gogurt-routes-s-d97ec4db50:6d9f164337 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Configuration Documents](index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-f4386b98bf"></a>
- <a id="s-9d6bc1bde7"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-281b9c9044"></a>`kind` | yes | type="string"; const="gogurt.routes" |  |
| <a id="s-cb8e5e292c"></a>`routes` | yes | type="object"; additional keys=`additionalProperties`, `propertyNames` |  |
| <a id="s-c5ff414687"></a>`schema_version` | yes | type="integer"; const=1 |  |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"gogurt-core:configuration:gogurt-routes-schema"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-787fb41e83"></a>[field routes · additional values · field command](#s-cb8e5e292c) | `cardinality · items · operational_policy` | shared above |
| [field routes](#s-cb8e5e292c) | `cardinality · entries · operational_policy` | shared above |

## Governing policies

- <a id="pa-48db652310"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-c6a5b8bc5d"></a>[extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration:gogurt-core:configuration:gogurt-routes-schema](../../../evidence/sources.md#src-75f9616966) — `reference/gogurt/packages/core/src/gogurt_core/core.py::GOGURT_ROUTES_SCHEMA`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/gogurt-core:configuration:gogurt-routes-schema`

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
