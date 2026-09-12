# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-8faff49cd7:bb329b4384 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `file_attributes` | no | type="integer"; minimum=0 |  |
| `file_name` | no | type="string" |  |
| `file_name_role` | no | enum=["exact","display"] |  |
| `file_name_utf16le_base64` | no | type="string"; additional keys=`contentEncoding` |  |
| `file_reference_number` | no | type="string"; pattern="^[0-9a-f]+$" |  |
| `major_version` | yes | type="integer"; minimum=0 |  |
| `minor_version` | yes | type="integer"; minimum=0 |  |
| `parent_file_reference_number` | no | type="string"; pattern="^[0-9a-f]+$" |  |
| `parse_status` | yes | enum=["parsed","unresolved","unsupported_version"] |  |
| `record_length` | yes | type="integer"; minimum=0 |  |
| `security_id` | no | type="integer"; minimum=0 |  |
| `usn` | no | type="string"; pattern="^-?(?:0\|[1-9][0-9]*)$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
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
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-usn-record.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-usn-record.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ccf844e8f4946745670a67f5dc85a9302e6bb0fe8feb65132592898e18221c8b -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "file_attributes": {
      "minimum": 0,
      "type": "integer"
    },
    "file_name": {
      "type": "string"
    },
    "file_name_role": {
      "enum": [
        "exact",
        "display"
      ]
    },
    "file_name_utf16le_base64": {
      "contentEncoding": "base64",
      "type": "string"
    },
    "file_reference_number": {
      "pattern": "^[0-9a-f]+$",
      "type": "string"
    },
    "major_version": {
      "minimum": 0,
      "type": "integer"
    },
    "minor_version": {
      "minimum": 0,
      "type": "integer"
    },
    "parent_file_reference_number": {
      "pattern": "^[0-9a-f]+$",
      "type": "string"
    },
    "parse_status": {
      "enum": [
        "parsed",
        "unresolved",
        "unsupported_version"
      ]
    },
    "record_length": {
      "minimum": 0,
      "type": "integer"
    },
    "security_id": {
      "minimum": 0,
      "type": "integer"
    },
    "usn": {
      "pattern": "^-?(?:0|[1-9][0-9]*)$",
      "type": "string"
    }
  },
  "required": [
    "record_length",
    "major_version",
    "minor_version",
    "parse_status"
  ],
  "type": "object"
}
```
