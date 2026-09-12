# Riverhog v1 immutable provenance root

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:riverhog-v1-immutable-provenance-root:d52d8bd3a4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-6adfbad66e) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-9ad4f8744f"></a>
- <a id="s-580bd3e8f6"></a>`$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json
- <a id="s-b9218f09af"></a>`title`: Riverhog v1 immutable provenance root
- <a id="s-30cda8dd75"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7f4eddd0b4"></a>`archive_generation` | yes | #/$defs/sha256 |  |
| <a id="s-d1c893d5e9"></a>`archive_tree_sha256` | yes | #/$defs/sha256 |  |
| <a id="s-1e57c64c67"></a>`schema` | yes | const="riverhog-provenance-root/v1" |  |
| <a id="s-b3f3b7c709"></a>`volume_sequence` | yes | type="object"; fields=`sha256`; additional keys=`additionalProperties`, `required` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-99767cd030"></a>`sha256` | type="string"; pattern="^[0-9a-f]{64}$" |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition sha256](#s-99767cd030) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-f7be4c7220"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-fb56828b2b"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json](../../../evidence/sources.md#src-7c2ebe352d) — `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-root-v1.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-provenance-root-v1.schema.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0eb6342ee7df1508833103be13d0a2cce27af43f40106bf135e49c7456445827 -->

```json
{
  "$defs": {
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "archive_generation": {
      "$ref": "#/$defs/sha256"
    },
    "archive_tree_sha256": {
      "$ref": "#/$defs/sha256"
    },
    "schema": {
      "const": "riverhog-provenance-root/v1"
    },
    "volume_sequence": {
      "additionalProperties": false,
      "properties": {
        "sha256": {
          "$ref": "#/$defs/sha256"
        }
      },
      "required": [
        "sha256"
      ],
      "type": "object"
    }
  },
  "required": [
    "schema",
    "archive_generation",
    "archive_tree_sha256",
    "volume_sequence"
  ],
  "title": "Riverhog v1 immutable provenance root",
  "type": "object"
}
```
