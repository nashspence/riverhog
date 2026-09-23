# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-provenance:https-nashspence-github-io-riverhog-v1-pr-15ee50803f:27ebac5716 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-355a3bb2b9"></a>

- <a id="s-099f69e6ee"></a>`type`: `"object"`
- <a id="s-22f558f449"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json"`
- <a id="s-ea2eb953be"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-5521758056"></a>`additionalProperties`: `false`
- <a id="s-67e1b84e5f"></a>`required`: `["complete","extents"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-54706e967d"></a>`complete` | yes | type="boolean" |  |
| `extents` | yes | [See field `extents`](#s-47d52016f8) |  |

### <a id="s-47d52016f8"></a>field `extents`

- <a id="s-c0203775cf"></a>`type`: `"array"`
- `items`: [See field `extents` · `items`](#s-030f8d4339)

### <a id="s-030f8d4339"></a>field `extents` · `items`

- <a id="s-84b951528e"></a>`type`: `"object"`
- <a id="s-2b32c1ff23"></a>`additionalProperties`: `false`
- <a id="s-c9a5b779b2"></a>`required`: `["kind","offset","length"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-077f632b1c"></a>`kind` | yes | enum=["data","hole"] |  |
| <a id="s-efe1d55da1"></a>`length` | yes | type="integer"; minimum=0 |  |
| <a id="s-6d97c28b99"></a>`offset` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field extents · items · field length](#s-efe1d55da1) | `value · schema-value · operational_policy` | shared above |
| [field extents · items · field offset](#s-6d97c28b99) | `value · schema-value · operational_policy` | shared above |
| [field extents](#s-47d52016f8) | `cardinality · items · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a849b03c15"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-4fe0210c0e"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json](../../../evidence/sources/authorities.md#src-5ae7bcd86c) — [packages/riverhog-provenance/src/riverhog\_provenance/schemas/sparse-map.schema.json](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/schemas/sparse-map.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1sparse-map.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac19cca6ba66bef85e5e0f2a52be5ef2fc630a83d507ef595009be8e534ad3eb -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "complete": {
      "type": "boolean"
    },
    "extents": {
      "items": {
        "additionalProperties": false,
        "properties": {
          "kind": {
            "enum": [
              "data",
              "hole"
            ]
          },
          "length": {
            "minimum": 0,
            "type": "integer"
          },
          "offset": {
            "minimum": 0,
            "type": "integer"
          }
        },
        "required": [
          "kind",
          "offset",
          "length"
        ],
        "type": "object"
      },
      "type": "array"
    }
  },
  "required": [
    "complete",
    "extents"
  ],
  "type": "object"
}
```

</details>
