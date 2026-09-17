# Riverhog v1 authenticated provenance-volume terminator

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-provenance:riverhog-v1-authenticated-provenance-volu-6f487d528f:5b13926a89 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-0e73294d13"></a>

- <a id="s-ae0dedb65a"></a>`type`: `"object"`
- <a id="s-de133b2f0e"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json"`
- <a id="s-9e561beee4"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-4bd3515c88"></a>`additionalProperties`: `false`
- <a id="s-f41155c31a"></a>`required`: `["schema","archive_generation","archive_tree_sha256","sequence","kind"]`
- <a id="s-832bea469a"></a>`title`: `"Riverhog v1 authenticated provenance-volume terminator"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-05d816de8d"></a>`archive_generation` | yes | [sha256](#s-0018c16833) |  |
| <a id="s-8ae66dd16b"></a>`archive_tree_sha256` | yes | [sha256](#s-0018c16833) |  |
| <a id="s-22621b37e7"></a>`kind` | yes | const="terminal" |  |
| <a id="s-3aa13d4fcd"></a>`schema` | yes | const="riverhog-provenance-terminal/v1" |  |
| <a id="s-6ce6f4fbfb"></a>`sequence` | yes | type="string"; not=(const="0000000000000000000000000000000000000000000000000000000000000000"); pattern="^[0-9a-f]{64}$" |  |

### Definitions

- [sha256](#s-0018c16833)

### <a id="s-0018c16833"></a>definition `sha256`

- <a id="s-e918d60570"></a>`type`: `"string"`
- <a id="s-ceffecee85"></a>`pattern`: `"^[0-9a-f]{64}$"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition sha256](#s-0018c16833) | `length · characters · fixed` | shared above |
| [field sequence](#s-6ce6f4fbfb) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-35e953f930"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-23f24d2291"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json](../../../evidence/sources/authorities.md#src-a639df0ba1) — [packages/riverhog-provenance/src/riverhog\_provenance/schemas/riverhog-provenance-terminal-v1.schema.json](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-terminal-v1.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-provenance-terminal-v1.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
