# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-9f1d17839c:c6eda73b5b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-78a2bea4a4b2) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-865162e45c23"></a>
- <a id="s-445c37d54bf2"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json
- <a id="s-d419668e4014"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f8a6a9cc1582"></a>`bytes_per_sector` | yes | type="integer"; minimum=0 |  |
| <a id="s-f4aa4baa11a9"></a>`drive_type` | yes | type="integer"; minimum=0 |  |
| <a id="s-c3f5ef01b7a4"></a>`filesystem_flags` | yes | type="integer"; minimum=0 |  |
| <a id="s-82189c6be46e"></a>`filesystem_name` | yes | type="string"; minLength=1 |  |
| <a id="s-d2a74a95e41b"></a>`final_path` | yes | type="string" |  |
| <a id="s-1c69c5fbb36f"></a>`maximum_component_length` | yes | type="integer"; minimum=0 |  |
| <a id="s-3fb9be6dfe52"></a>`mount_path` | yes | type="string" |  |
| <a id="s-a766697b2b84"></a>`sectors_per_cluster` | yes | type="integer"; minimum=0 |  |
| <a id="s-dd857771a2ea"></a>`volume_guid_path` | yes | type="string" |  |
| <a id="s-a2f9ecad73b7"></a>`volume_label` | yes | type="string" |  |
| <a id="s-48e6f48c844a"></a>`volume_serial_number` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes_per_sector](#s-f8a6a9cc1582) | `value · schema-value · extension_owned` | shared above |
| [field maximum_component_length](#s-1c69c5fbb36f) | `value · schema-value · extension_owned` | shared above |

## Governing policies

- <a id="pa-07d7e7d6f394"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-e74e6e53512e"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json](../../../evidence/sources.md#src-025e48b0abb0) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-volume-context.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-volume-context.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74c63c0471ae132d4e4c3c695e5cb9250813ff3c243d70d7636d8973ffb05481 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "bytes_per_sector": {
      "minimum": 0,
      "type": "integer"
    },
    "drive_type": {
      "minimum": 0,
      "type": "integer"
    },
    "filesystem_flags": {
      "minimum": 0,
      "type": "integer"
    },
    "filesystem_name": {
      "minLength": 1,
      "type": "string"
    },
    "final_path": {
      "type": "string"
    },
    "maximum_component_length": {
      "minimum": 0,
      "type": "integer"
    },
    "mount_path": {
      "type": "string"
    },
    "sectors_per_cluster": {
      "minimum": 0,
      "type": "integer"
    },
    "volume_guid_path": {
      "type": "string"
    },
    "volume_label": {
      "type": "string"
    },
    "volume_serial_number": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "filesystem_name",
    "volume_label",
    "volume_serial_number",
    "maximum_component_length",
    "filesystem_flags",
    "mount_path",
    "volume_guid_path",
    "drive_type",
    "sectors_per_cluster",
    "bytes_per_sector",
    "final_path"
  ],
  "type": "object"
}
```
