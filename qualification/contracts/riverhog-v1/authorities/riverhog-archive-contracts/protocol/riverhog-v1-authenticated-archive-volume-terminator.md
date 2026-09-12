# Riverhog v1 authenticated archive-volume terminator

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-archive-contracts:riverhog-v1-authenticated-archive-volume-terminator:66bc9ebfc2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-66d562e63c) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-ff6135e233"></a>
- <a id="s-edab20a12b"></a>`$id`: https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json
- <a id="s-c5d4fb8739"></a>`title`: Riverhog v1 authenticated archive-volume terminator
- <a id="s-1c6f4770a5"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0cad81e76e"></a>`archive_generation` | yes | #/$defs/sha256 |  |
| <a id="s-6108f7d82d"></a>`archive_tree_sha256` | yes | #/$defs/sha256 |  |
| <a id="s-814acfeac2"></a>`kind` | yes | const="terminal" |  |
| <a id="s-7a9a2071e4"></a>`schema` | yes | const="collection-archive-terminal/v1" |  |
| <a id="s-c9a5588471"></a>`sequence` | yes | type="string"; pattern="^[0-9a-f]{64}$"; additional keys=`not` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-58fcc75c8a"></a>`sha256` | type="string"; pattern="^[0-9a-f]{64}$" |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition sha256](#s-58fcc75c8a) | `length · characters · fixed` | shared above |
| [field sequence](#s-c9a5588471) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-c144b5efa8"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-7472f137fb"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json](../../../evidence/sources.md#src-c45ad44582) — `packages/riverhog-archive-contracts/schemas/collection-archive-terminal-v1.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1collection-archive-terminal-v1.schema.json`

### Exact owned JSON

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
