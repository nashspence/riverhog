# Riverhog v1 authenticated provenance-volume terminator

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:riverhog-v1-authenticated-provenance-volu-6f487d528f:e5302da766 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-6adfbad66ec5) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-0e73294d1380"></a>
- <a id="s-de133b2f0e98"></a>`$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json
- <a id="s-832bea469a9b"></a>`title`: Riverhog v1 authenticated provenance-volume terminator
- <a id="s-ae0dedb65a4b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-05d816de8db4"></a>`archive_generation` | yes | #/$defs/sha256 |  |
| <a id="s-8ae66dd16bb5"></a>`archive_tree_sha256` | yes | #/$defs/sha256 |  |
| <a id="s-22621b37e74f"></a>`kind` | yes | const="terminal" |  |
| <a id="s-3aa13d4fcda3"></a>`schema` | yes | const="riverhog-provenance-terminal/v1" |  |
| <a id="s-6ce6f4fbfb26"></a>`sequence` | yes | type="string"; pattern="^[0-9a-f]{64}$"; additional keys=`not` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-0018c16833bb"></a>`sha256` | type="string"; pattern="^[0-9a-f]{64}$" |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition sha256](#s-0018c16833bb) | `length · characters · fixed` | shared above |
| [field sequence](#s-6ce6f4fbfb26) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-4803089bcb6d"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-96f460c70446"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json](../../../evidence/sources.md#src-a639df0ba137) — `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-terminal-v1.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-provenance-terminal-v1.schema.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0adc252e32d2acdca092eeb7a30601d8ce840fa83722504eb594598f501ee97d -->

```json
{
  "$defs": {
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json",
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
      "const": "riverhog-provenance-terminal/v1"
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
  "title": "Riverhog v1 authenticated provenance-volume terminator",
  "type": "object"
}
```
