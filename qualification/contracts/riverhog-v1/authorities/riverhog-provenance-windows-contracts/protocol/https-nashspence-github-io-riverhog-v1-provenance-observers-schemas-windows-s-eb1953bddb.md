# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-eb1953bddb:459d9fbc6e -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-security-descriptor.json`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-security-descriptor.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `control` | yes | integer |  |
| `group_sid` | yes | string |  |
| `owner_sid` | yes | string |  |
| `sacl_included` | yes | boolean |  |
| `security_information` | yes | integer |  |
