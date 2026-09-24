# Riverhog v1 immutable provenance root

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-provenance:riverhog-v1-immutable-provenance-root:93cdc3aa00 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-9ad4f8744f"></a>

- <a id="s-30cda8dd75"></a>`type`: `"object"`
- <a id="s-580bd3e8f6"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json"`
- <a id="s-109fc739da"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-f6f0f83de3"></a>`additionalProperties`: `false`
- <a id="s-caf8e7a4f6"></a>`required`: `["format","archive_generation","archive_tree_sha256","volume_sequence"]`
- <a id="s-b9218f09af"></a>`title`: `"Riverhog v1 immutable provenance root"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7f4eddd0b4"></a>`archive_generation` | yes | [sha256](#s-99767cd030) |  |
| <a id="s-d1c893d5e9"></a>`archive_tree_sha256` | yes | [sha256](#s-99767cd030) |  |
| <a id="s-91dba28e3e"></a>`format` | yes | const="riverhog-provenance-root/v1" |  |
| `volume_sequence` | yes | [See field `volume_sequence`](#s-b3f3b7c709) |  |

### Definitions

- [sha256](#s-99767cd030)

### <a id="s-b3f3b7c709"></a>field `volume_sequence`

- <a id="s-a1364476e8"></a>`type`: `"object"`
- <a id="s-fbb3b5bae2"></a>`additionalProperties`: `false`
- <a id="s-8467aaa008"></a>`required`: `["sha256"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e3666b6108"></a>`sha256` | yes | [sha256](#s-99767cd030) |  |

### <a id="s-99767cd030"></a>definition `sha256`

- <a id="s-1f6e98ee38"></a>`type`: `"string"`
- <a id="s-7b72addc6a"></a>`pattern`: `"^[0-9a-f]{64}$"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition sha256](#s-99767cd030) | `length · characters · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c560b8e596"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-99050d3f45"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json](../../../evidence/sources/authorities.md#src-7c2ebe352d) — [packages/riverhog-provenance/src/riverhog\_provenance/schemas/riverhog-provenance-root-v1.schema.json](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-root-v1.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-provenance-root-v1.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 62aee778953c6fa194b557dcc15dcd2755f60e60980fed09812cdeb9b9a77dc4 -->

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
    "format": {
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
    "format",
    "archive_generation",
    "archive_tree_sha256",
    "volume_sequence"
  ],
  "title": "Riverhog v1 immutable provenance root",
  "type": "object"
}
```

</details>
