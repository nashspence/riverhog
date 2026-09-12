# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-d75497e2ae:f0dfbb037a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `followed_for_primary_content` | yes | type="boolean" |  |
| `name_surrogate` | yes | type="boolean" |  |
| `reparse_tag` | yes | type="integer"; minimum=0 |  |
| `reparse_tag_hex` | yes | type="string"; pattern="^0x[0-9a-f]{8}$" |  |

## Governing policies

- `compatibility/components/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-reparse-point.schema.json`

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
