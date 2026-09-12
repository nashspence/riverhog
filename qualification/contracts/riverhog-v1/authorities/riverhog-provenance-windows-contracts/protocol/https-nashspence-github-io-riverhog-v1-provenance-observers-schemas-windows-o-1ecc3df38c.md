# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-1ecc3df38c:6636f9a6fb -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-object-id.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-object-id.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=32, minimum=32, reason=fixed-public-representation |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `extended_info` | yes | string |  |
| `object_id` | yes | string |  |

## Complete owned contract

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
