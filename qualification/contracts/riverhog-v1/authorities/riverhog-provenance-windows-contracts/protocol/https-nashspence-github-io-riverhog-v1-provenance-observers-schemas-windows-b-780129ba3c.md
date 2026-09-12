# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-780129ba3c:680fb2d9ea -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `ea_flags` | no | type="integer"; minimum=0 |  |
| `need_ea` | no | type="boolean" |  |
| `stream_attribute_names` | yes | type="array"; items=(type="string"; minLength=1); additional keys=`uniqueItems` |  |
| `stream_attributes` | yes | type="integer"; minimum=0 |  |
| `stream_id` | yes | type="integer"; minimum=0 |  |
| `stream_size` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Governing policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-backup-stream-info.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-backup-stream-info.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5749ab8c4944e69d57543fc32f3be4ed3f02eb66c903884166f368a98a0e4b44 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "ea_flags": {
      "minimum": 0,
      "type": "integer"
    },
    "need_ea": {
      "type": "boolean"
    },
    "stream_attribute_names": {
      "items": {
        "minLength": 1,
        "type": "string"
      },
      "type": "array",
      "uniqueItems": true
    },
    "stream_attributes": {
      "minimum": 0,
      "type": "integer"
    },
    "stream_id": {
      "minimum": 0,
      "type": "integer"
    },
    "stream_size": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "stream_id",
    "stream_attributes",
    "stream_attribute_names",
    "stream_size"
  ],
  "type": "object"
}
```
