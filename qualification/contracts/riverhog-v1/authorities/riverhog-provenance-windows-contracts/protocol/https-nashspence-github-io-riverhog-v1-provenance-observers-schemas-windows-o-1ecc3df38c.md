# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-1ecc3df38c:6636f9a6fb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-78a2bea4a4b2) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-bf0784e7cb19"></a>
- <a id="s-bc6e6a1ac77f"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json
- <a id="s-e53f3faedf41"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0f71aad8612e"></a>`extended_info` | yes | type="string"; pattern="^[0-9a-f]{0,96}$" |  |
| <a id="s-660bc1b26b5d"></a>`object_id` | yes | type="string"; pattern="^[0-9a-f]{32}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=32; minimum=32; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{32}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field object_id](#s-660bc1b26b5d) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-33d31c892df8"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-c1dff382b4ba"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json](../../../evidence/sources.md#src-76f06f0f43dc) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-object-id.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-object-id.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 430a561b65a9b122a328d3b778f6554d4595b652086d22adef84174b8d892a00 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "extended_info": {
      "pattern": "^[0-9a-f]{0,96}$",
      "type": "string"
    },
    "object_id": {
      "pattern": "^[0-9a-f]{32}$",
      "type": "string"
    }
  },
  "required": [
    "object_id",
    "extended_info"
  ],
  "type": "object"
}
```
