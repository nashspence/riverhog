# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-d75497e2ae:f0dfbb037a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-reparse-point.json`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-reparse-point.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `followed_for_primary_content` | yes | boolean |  |
| `name_surrogate` | yes | boolean |  |
| `reparse_tag` | yes | integer |  |
| `reparse_tag_hex` | yes | string |  |
