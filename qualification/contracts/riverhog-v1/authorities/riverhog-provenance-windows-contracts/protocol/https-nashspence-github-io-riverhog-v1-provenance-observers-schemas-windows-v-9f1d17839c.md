# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-9f1d17839c:c6eda73b5b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-volume-context.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-volume-context.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `bytes_per_sector` | yes | integer |  |
| `drive_type` | yes | integer |  |
| `filesystem_flags` | yes | integer |  |
| `filesystem_name` | yes | string |  |
| `final_path` | yes | string |  |
| `maximum_component_length` | yes | integer |  |
| `mount_path` | yes | string |  |
| `sectors_per_cluster` | yes | integer |  |
| `volume_guid_path` | yes | string |  |
| `volume_label` | yes | string |  |
| `volume_serial_number` | yes | integer |  |
