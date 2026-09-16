# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-448544fd06:d7babc419c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-fb6062ee71"></a>

- <a id="s-704cd8c542"></a>`type`: `"object"`
- <a id="s-e625ee524c"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json"`
- <a id="s-623ed0a5f6"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-8a90b22b6c"></a>`additionalProperties`: `false`
- <a id="s-c487271d99"></a>`required`: `["volume_serial_number","file_id_hex","file_id_bits","file_id_scheme","file_index_64","creation_time_ticks","last_access_time_ticks","last_write_time_ticks","change_time_ticks","file_attributes","reparse_tag","allocation_size","end_of_file","number_of_links","delete_pending","storage"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7fec217a4b"></a>`allocation_size` | yes | type="integer"; minimum=0 |  |
| <a id="s-21e7cfe7a5"></a>`change_time_ticks` | yes | type="integer" |  |
| <a id="s-e763f8847b"></a>`creation_time_ticks` | yes | type="integer" |  |
| <a id="s-e8e9d915da"></a>`delete_pending` | yes | type="boolean" |  |
| <a id="s-78fae05564"></a>`end_of_file` | yes | type="integer"; minimum=0 |  |
| <a id="s-e912033c6c"></a>`file_attributes` | yes | type="integer"; minimum=0 |  |
| <a id="s-d765965d60"></a>`file_id_bits` | yes | type="integer"; enum=[64,128] |  |
| <a id="s-f3b6363d9b"></a>`file_id_hex` | yes | type="string"; pattern="^[0-9a-f]+$" |  |
| <a id="s-2a7ab9fe85"></a>`file_id_scheme` | yes | enum=["windows-file-id-128","windows-file-index-64"] |  |
| <a id="s-f85d98af56"></a>`file_index_64` | yes | type="integer"; minimum=0 |  |
| <a id="s-1f160fef61"></a>`last_access_time_ticks` | yes | type="integer" |  |
| <a id="s-1f4002e73c"></a>`last_write_time_ticks` | yes | type="integer" |  |
| <a id="s-3c314f81c8"></a>`number_of_links` | yes | type="integer"; minimum=0 |  |
| <a id="s-f2a08f0300"></a>`reparse_tag` | yes | type="integer"; minimum=0 |  |
| `storage` | yes | [See field `storage`](#s-59bba6e348) |  |
| <a id="s-27bddf3c48"></a>`volume_serial_number` | yes | type="integer"; minimum=0 |  |

### <a id="s-59bba6e348"></a>field `storage`

- <a id="s-85d91220f8"></a>`type`: `"object"`
- <a id="s-02a5031da1"></a>`additionalProperties`: `false`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e3f23ed149"></a>`byte_offset_for_partition_alignment` | no | type="integer"; minimum=0 |  |
| <a id="s-5d167ac729"></a>`byte_offset_for_sector_alignment` | no | type="integer"; minimum=0 |  |
| <a id="s-e93e4f749a"></a>`filesystem_effective_physical_bytes_per_sector_for_atomicity` | no | type="integer"; minimum=0 |  |
| <a id="s-fdf617d5e5"></a>`flags` | no | type="integer"; minimum=0 |  |
| <a id="s-bee06254ad"></a>`logical_bytes_per_sector` | no | type="integer"; minimum=0 |  |
| <a id="s-5c09c76c78"></a>`physical_bytes_per_sector_for_atomicity` | no | type="integer"; minimum=0 |  |
| <a id="s-0ddc665411"></a>`physical_bytes_per_sector_for_performance` | no | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field allocation_size](#s-7fec217a4b) | `value · schema-value · extension_owned` | shared above |
| [field end_of_file](#s-78fae05564) | `value · schema-value · extension_owned` | shared above |
| [field file_attributes](#s-e912033c6c) | `value · schema-value · extension_owned` | shared above |
| [field file_id_bits](#s-d765965d60) | `value · schema-value · extension_owned` | shared above |
| [field file_index_64](#s-f85d98af56) | `value · schema-value · extension_owned` | shared above |
| [field storage · field byte_offset_for_partition_alignment](#s-e3f23ed149) | `value · schema-value · extension_owned` | shared above |
| [field storage · field byte_offset_for_sector_alignment](#s-5d167ac729) | `value · schema-value · extension_owned` | shared above |
| [field storage · field filesystem_effective_physical_bytes_per_sector_for_atomicity](#s-e93e4f749a) | `value · schema-value · extension_owned` | shared above |
| [field storage · field logical_bytes_per_sector](#s-bee06254ad) | `value · schema-value · extension_owned` | shared above |
| [field storage · field physical_bytes_per_sector_for_atomicity](#s-5c09c76c78) | `value · schema-value · extension_owned` | shared above |
| [field storage · field physical_bytes_per_sector_for_performance](#s-0ddc665411) | `value · schema-value · extension_owned` | shared above |

## Governing policies

- <a id="pa-31ca6f4071"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-c7ef971e53"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json](../../../evidence/sources.md#src-232ad4e832) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-file-stat.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-file-stat.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7e1f4e64443ea706e814ef0b3eb7e5767f43c246072c6e8a14b3787b0d5aa7a -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "allocation_size": {
      "minimum": 0,
      "type": "integer"
    },
    "change_time_ticks": {
      "type": "integer"
    },
    "creation_time_ticks": {
      "type": "integer"
    },
    "delete_pending": {
      "type": "boolean"
    },
    "end_of_file": {
      "minimum": 0,
      "type": "integer"
    },
    "file_attributes": {
      "minimum": 0,
      "type": "integer"
    },
    "file_id_bits": {
      "enum": [
        64,
        128
      ],
      "type": "integer"
    },
    "file_id_hex": {
      "pattern": "^[0-9a-f]+$",
      "type": "string"
    },
    "file_id_scheme": {
      "enum": [
        "windows-file-id-128",
        "windows-file-index-64"
      ]
    },
    "file_index_64": {
      "minimum": 0,
      "type": "integer"
    },
    "last_access_time_ticks": {
      "type": "integer"
    },
    "last_write_time_ticks": {
      "type": "integer"
    },
    "number_of_links": {
      "minimum": 0,
      "type": "integer"
    },
    "reparse_tag": {
      "minimum": 0,
      "type": "integer"
    },
    "storage": {
      "additionalProperties": false,
      "properties": {
        "byte_offset_for_partition_alignment": {
          "minimum": 0,
          "type": "integer"
        },
        "byte_offset_for_sector_alignment": {
          "minimum": 0,
          "type": "integer"
        },
        "filesystem_effective_physical_bytes_per_sector_for_atomicity": {
          "minimum": 0,
          "type": "integer"
        },
        "flags": {
          "minimum": 0,
          "type": "integer"
        },
        "logical_bytes_per_sector": {
          "minimum": 0,
          "type": "integer"
        },
        "physical_bytes_per_sector_for_atomicity": {
          "minimum": 0,
          "type": "integer"
        },
        "physical_bytes_per_sector_for_performance": {
          "minimum": 0,
          "type": "integer"
        }
      },
      "type": "object"
    },
    "volume_serial_number": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "volume_serial_number",
    "file_id_hex",
    "file_id_bits",
    "file_id_scheme",
    "file_index_64",
    "creation_time_ticks",
    "last_access_time_ticks",
    "last_write_time_ticks",
    "change_time_ticks",
    "file_attributes",
    "reparse_tag",
    "allocation_size",
    "end_of_file",
    "number_of_links",
    "delete_pending",
    "storage"
  ],
  "type": "object"
}
```

</details>
