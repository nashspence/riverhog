# gogurt-core:configuration:gogurt-routes-schema configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:gogurt-core:gogurt-core-configuration-gogurt-routes-s-d97ec4db50:6d9f164337 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-f4386b98bf"></a>

- <a id="s-9d6bc1bde7"></a>`type`: `"object"`
- <a id="s-7bbeca7b57"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-43146c76c3"></a>`additionalProperties`: `false`
- <a id="s-90d1d0dea2"></a>`required`: `["schema_version","kind","routes"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-281b9c9044"></a>`kind` | yes | type="string"; const="gogurt.routes" |  |
| `routes` | yes | [See field `routes`](#s-cb8e5e292c) |  |
| <a id="s-c5ff414687"></a>`schema_version` | yes | type="integer"; const=1 |  |

### <a id="s-cb8e5e292c"></a>field `routes`

- <a id="s-175a552f6b"></a>`type`: `"object"`
- `additionalProperties`: [See field `routes` · `additionalProperties`](#s-c6de0d0ee5)
- <a id="s-83d917ba74"></a>`propertyNames`: pattern="^[a-z0-9]&#40;?:[a-z0-9-]{0,61}[a-z0-9])?$"

### <a id="s-c6de0d0ee5"></a>field `routes` · `additionalProperties`

- <a id="s-bed06600de"></a>`type`: `"object"`
- <a id="s-2ec69efabc"></a>`additionalProperties`: `false`
- <a id="s-1a943779ea"></a>`required`: `["command"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-787fb41e83"></a>`command` | yes | type="array"; items=(type="string"; minLength=1); minItems=1 |  |
| <a id="s-5ad47b718e"></a>`enabled` | no | type="boolean" |  |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"gogurt-core:configuration:gogurt-routes-schema"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field routes · additional values · field command](#s-787fb41e83) | `cardinality · items · operational_policy` | shared above |
| [field routes](#s-cb8e5e292c) | `cardinality · entries · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-48db652310"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-c6a5b8bc5d"></a>[extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:gogurt-core:configuration:gogurt-routes-schema](../../../evidence/sources/authorities.md#src-75f9616966) — [some-implementations/gogurt/packages/core/src/gogurt\_core/core.py::GOGURT\_ROUTES\_SCHEMA](../../../../../../some-implementations/gogurt/packages/core/src/gogurt_core/core.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/gogurt-core:configuration:gogurt-routes-schema`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
