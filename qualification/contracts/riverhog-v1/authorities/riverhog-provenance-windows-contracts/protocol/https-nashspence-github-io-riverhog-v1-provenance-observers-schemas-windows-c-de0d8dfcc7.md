# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-de0d8dfcc7:68551531bd -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-compression-state.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-compression-state.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `chunk_shift` | yes | integer |  |
| `cluster_shift` | yes | integer |  |
| `compressed_size` | yes | integer |  |
| `compression_format` | yes | integer |  |
| `compression_format_name` | yes | string |  |
| `compression_unit_shift` | yes | integer |  |
| `file_attribute_compressed` | yes | boolean |  |
