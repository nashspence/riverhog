# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-2f54ac7817:bb41f2a4f9 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-file-attributes.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-file-attributes.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `bitmask` | yes | integer |  |
| `hex` | yes | string |  |
| `names` | yes | array |  |
| `reparse_tag` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37bba6822376342893fc3815b03e3da54ba3828bdbeb7cfe4fa6cd763a3055d4 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "bitmask": {
      "minimum": 0,
      "type": "integer"
    },
    "hex": {
      "pattern": "^0x[0-9a-f]{8}$",
      "type": "string"
    },
    "names": {
      "items": {
        "minLength": 1,
        "type": "string"
      },
      "type": "array",
      "uniqueItems": true
    },
    "reparse_tag": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "bitmask",
    "hex",
    "names",
    "reparse_tag"
  ],
  "type": "object"
}
```
