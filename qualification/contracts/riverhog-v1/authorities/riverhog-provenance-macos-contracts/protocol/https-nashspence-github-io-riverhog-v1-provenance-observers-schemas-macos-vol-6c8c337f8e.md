# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-macos-contracts:https-nashspence-github-io-riverhog-v1-pr-6c8c337f8e:aa33cf5f83 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-macos-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `block_size` | yes | type="integer"; minimum=0 |  |
| `capabilities` | no | type="array"; items=(type="integer"; minimum=0) |  |
| `filesystem_subtype` | yes | type="integer"; minimum=0 |  |
| `filesystem_type` | yes | type="string"; minLength=1 |  |
| `fsid` | yes | type="array"; items=(false); additional keys=`prefixItems` |  |
| `io_size` | yes | type="integer"; minimum=0 |  |
| `mount_flags` | yes | type="integer"; minimum=0 |  |
| `mount_point` | yes | type="string" |  |
| `mounted_from` | yes | type="string" |  |
| `valid_capabilities` | no | type="array"; items=(type="integer"; minimum=0) |  |
| `volume_uuid` | no | type="string"; format="uuid"; pattern="^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Governing policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json` — `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/macos-volume-context.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1macos-volume-context.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61053cf50b05a17a636ea109f39ec9fd2749d14271871a9eee5d83775112e933 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "block_size": {
      "minimum": 0,
      "type": "integer"
    },
    "capabilities": {
      "items": {
        "minimum": 0,
        "type": "integer"
      },
      "type": "array"
    },
    "filesystem_subtype": {
      "minimum": 0,
      "type": "integer"
    },
    "filesystem_type": {
      "minLength": 1,
      "type": "string"
    },
    "fsid": {
      "items": false,
      "prefixItems": [
        {
          "type": "integer"
        },
        {
          "type": "integer"
        }
      ],
      "type": "array"
    },
    "io_size": {
      "minimum": 0,
      "type": "integer"
    },
    "mount_flags": {
      "minimum": 0,
      "type": "integer"
    },
    "mount_point": {
      "type": "string"
    },
    "mounted_from": {
      "type": "string"
    },
    "valid_capabilities": {
      "items": {
        "minimum": 0,
        "type": "integer"
      },
      "type": "array"
    },
    "volume_uuid": {
      "format": "uuid",
      "pattern": "^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$",
      "type": "string"
    }
  },
  "required": [
    "filesystem_type",
    "mount_point",
    "mounted_from",
    "fsid",
    "mount_flags",
    "filesystem_subtype",
    "io_size",
    "block_size"
  ],
  "type": "object"
}
```
