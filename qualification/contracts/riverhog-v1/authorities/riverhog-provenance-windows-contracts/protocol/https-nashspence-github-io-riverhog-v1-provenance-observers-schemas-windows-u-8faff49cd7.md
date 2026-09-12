# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-8faff49cd7:bb329b4384 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-78a2bea4a4) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-740b4aabb0"></a>
- <a id="s-6ad7b66791"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json
- <a id="s-e652a64a5e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2384c7e8e7"></a>`file_attributes` | no | type="integer"; minimum=0 |  |
| <a id="s-d0ea6b8d9c"></a>`file_name` | no | type="string" |  |
| <a id="s-231ea73abe"></a>`file_name_role` | no | enum=["exact","display"] |  |
| <a id="s-5f44cc2a52"></a>`file_name_utf16le_base64` | no | type="string"; additional keys=`contentEncoding` |  |
| <a id="s-d9be3c10e6"></a>`file_reference_number` | no | type="string"; pattern="^[0-9a-f]+$" |  |
| <a id="s-88739230bb"></a>`major_version` | yes | type="integer"; minimum=0 |  |
| <a id="s-81947330b8"></a>`minor_version` | yes | type="integer"; minimum=0 |  |
| <a id="s-c222ed244a"></a>`parent_file_reference_number` | no | type="string"; pattern="^[0-9a-f]+$" |  |
| <a id="s-50832b9c1e"></a>`parse_status` | yes | enum=["parsed","unresolved","unsupported_version"] |  |
| <a id="s-68b80937e0"></a>`record_length` | yes | type="integer"; minimum=0 |  |
| <a id="s-def5c2bea6"></a>`security_id` | no | type="integer"; minimum=0 |  |
| <a id="s-322c894f7c"></a>`usn` | no | type="string"; pattern="^-?(?:0\|[1-9][0-9]*)$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field file_attributes](#s-2384c7e8e7) | `value · schema-value · extension_owned` | shared above |
| [field record_length](#s-68b80937e0) | `value · schema-value · extension_owned` | shared above |

## Governing policies

- <a id="pa-2859539811"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-937cdb16a4"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json](../../../evidence/sources.md#src-72281b7ad5) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-usn-record.schema.json`

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
