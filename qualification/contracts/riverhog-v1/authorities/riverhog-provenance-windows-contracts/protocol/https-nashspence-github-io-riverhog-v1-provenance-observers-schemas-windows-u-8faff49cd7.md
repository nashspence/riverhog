# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-8faff49cd7:bb329b4384 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-usn-record.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-usn-record.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `file_attributes` | no | integer |  |
| `file_name` | no | string |  |
| `file_name_role` | no | object (1 fields) |  |
| `file_name_utf16le_base64` | no | string |  |
| `file_reference_number` | no | string |  |
| `major_version` | yes | integer |  |
| `minor_version` | yes | integer |  |
| `parent_file_reference_number` | no | string |  |
| `parse_status` | yes | object (1 fields) |  |
| `record_length` | yes | integer |  |
| `security_id` | no | integer |  |
| `usn` | no | string |  |
