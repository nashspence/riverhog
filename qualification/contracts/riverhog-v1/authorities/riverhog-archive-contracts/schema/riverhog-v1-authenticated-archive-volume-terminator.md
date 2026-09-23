# Riverhog v1 authenticated archive-volume terminator

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-archive-contracts:riverhog-v1-authenticated-archive-volume-terminator:7c06ae39a1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-ff6135e233"></a>

- <a id="s-1c6f4770a5"></a>`type`: `"object"`
- <a id="s-531b070066"></a>`$comment`: `"This schema is the structural projection. riverhog_archive_contracts.CollectionArchiveTerminalDocument is the canonical semantic, identity, and canonical-JSON authority."`
- <a id="s-edab20a12b"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json"`
- <a id="s-93bda210a2"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-b2fe1e65e6"></a>`additionalProperties`: `false`
- <a id="s-71849fbcf8"></a>`required`: `["schema","archive_generation","archive_tree_sha256","sequence","kind"]`
- <a id="s-c5d4fb8739"></a>`title`: `"Riverhog v1 authenticated archive-volume terminator"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0cad81e76e"></a>`archive_generation` | yes | [sha256](#s-58fcc75c8a) |  |
| <a id="s-6108f7d82d"></a>`archive_tree_sha256` | yes | [sha256](#s-58fcc75c8a) |  |
| <a id="s-814acfeac2"></a>`kind` | yes | const="terminal" |  |
| <a id="s-7a9a2071e4"></a>`schema` | yes | const="collection-archive-terminal/v1" |  |
| <a id="s-c9a5588471"></a>`sequence` | yes | type="string"; not=(const="0000000000000000000000000000000000000000000000000000000000000000"); pattern="^[0-9a-f]{64}$" |  |

### Definitions

- [sha256](#s-58fcc75c8a)

### <a id="s-58fcc75c8a"></a>definition `sha256`

- <a id="s-01d9b5e600"></a>`type`: `"string"`
- <a id="s-4c2ea1cd7d"></a>`pattern`: `"^[0-9a-f]{64}$"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition sha256](#s-58fcc75c8a) | `length · characters · fixed` | shared above |
| [field sequence](#s-c9a5588471) | `length · characters · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-af4dac77d0"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-a60a7ed5d3"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json](../../../evidence/sources/authorities.md#src-c45ad44582) — [packages/riverhog-archive-contracts/schemas/collection-archive-terminal-v1.schema.json](../../../../../../packages/riverhog-archive-contracts/schemas/collection-archive-terminal-v1.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1collection-archive-terminal-v1.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c1f9e18d46186672c66a5798bae3eb362c1e8b9d8668c1dae2d67fe5836c8db -->

```json
{
  "$comment": "This schema is the structural projection. riverhog_archive_contracts.CollectionArchiveTerminalDocument is the canonical semantic, identity, and canonical-JSON authority.",
  "$defs": {
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "archive_generation": {
      "$ref": "#/$defs/sha256"
    },
    "archive_tree_sha256": {
      "$ref": "#/$defs/sha256"
    },
    "kind": {
      "const": "terminal"
    },
    "schema": {
      "const": "collection-archive-terminal/v1"
    },
    "sequence": {
      "not": {
        "const": "0000000000000000000000000000000000000000000000000000000000000000"
      },
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    }
  },
  "required": [
    "schema",
    "archive_generation",
    "archive_tree_sha256",
    "sequence",
    "kind"
  ],
  "title": "Riverhog v1 authenticated archive-volume terminator",
  "type": "object"
}
```

</details>
