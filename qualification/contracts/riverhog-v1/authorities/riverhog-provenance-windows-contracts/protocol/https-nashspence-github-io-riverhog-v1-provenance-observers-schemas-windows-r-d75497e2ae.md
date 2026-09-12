# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-d75497e2ae:f0dfbb037a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-78a2bea4a4b2) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-401d4b1864f7"></a>
- <a id="s-3f410425c237"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json
- <a id="s-4873d9739c41"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8ca03a7e2081"></a>`followed_for_primary_content` | yes | type="boolean" |  |
| <a id="s-b61d838937ec"></a>`name_surrogate` | yes | type="boolean" |  |
| <a id="s-187e4d4d8f96"></a>`reparse_tag` | yes | type="integer"; minimum=0 |  |
| <a id="s-928d94da05bd"></a>`reparse_tag_hex` | yes | type="string"; pattern="^0x[0-9a-f]{8}$" |  |

## Governing policies

- <a id="pa-3fbd0f8eb9fa"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json](../../../evidence/sources.md#src-7b71865af2c5) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-reparse-point.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-reparse-point.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6098b111132c6f51c8d317c0f4514814fbd97c875063fa81abfb96f01c80e77a -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "followed_for_primary_content": {
      "type": "boolean"
    },
    "name_surrogate": {
      "type": "boolean"
    },
    "reparse_tag": {
      "minimum": 0,
      "type": "integer"
    },
    "reparse_tag_hex": {
      "pattern": "^0x[0-9a-f]{8}$",
      "type": "string"
    }
  },
  "required": [
    "reparse_tag",
    "reparse_tag_hex",
    "name_surrogate",
    "followed_for_primary_content"
  ],
  "type": "object"
}
```
