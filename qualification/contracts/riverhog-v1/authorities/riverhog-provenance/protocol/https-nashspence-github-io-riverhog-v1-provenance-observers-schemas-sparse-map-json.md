# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:https-nashspence-github-io-riverhog-v1-pr-15ee50803f:cf9b0ef47a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-6adfbad66ec5) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-355a3bb2b9b1"></a>
- <a id="s-22f558f449f8"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json
- <a id="s-099f69e6ee30"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-54706e967d48"></a>`complete` | yes | type="boolean" |  |
| <a id="s-47d52016f860"></a>`extents` | yes | type="array"; items=(type="object"; fields=`kind`, `length`, `offset`; additional keys=`additionalProperties`, `required`) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-efe1d55da1d0"></a>field extents · items · field length | `value · schema-value · operational_policy` | shared above |
| <a id="s-6d97c28b9993"></a>field extents · items · field offset | `value · schema-value · operational_policy` | shared above |
| [field extents](#s-47d52016f860) | `cardinality · items · operational_policy` | shared above |

## Governing policies

- <a id="pa-788c4313b7c6"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-689f8a66f28a"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json](../../../evidence/sources.md#src-5ae7bcd86c0a) — `packages/riverhog-provenance/src/riverhog_provenance/schemas/sparse-map.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1sparse-map.json`

### Exact owned JSON

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
