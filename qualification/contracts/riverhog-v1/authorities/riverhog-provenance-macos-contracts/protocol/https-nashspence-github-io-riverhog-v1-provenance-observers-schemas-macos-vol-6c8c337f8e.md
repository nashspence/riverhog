# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-macos-contracts:https-nashspence-github-io-riverhog-v1-pr-6c8c337f8e:aa33cf5f83 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-macos-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 7 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1macos-volume-context.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json` — `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/macos-volume-context.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `block_size` | yes | integer |  |
| `capabilities` | no | array |  |
| `filesystem_subtype` | yes | integer |  |
| `filesystem_type` | yes | string |  |
| `fsid` | yes | array |  |
| `io_size` | yes | integer |  |
| `mount_flags` | yes | integer |  |
| `mount_point` | yes | string |  |
| `mounted_from` | yes | string |  |
| `valid_capabilities` | no | array |  |
| `volume_uuid` | no | string |  |
