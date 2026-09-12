# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-linux-contracts:https-nashspence-github-io-riverhog-v1-pr-453aae6637:501bf68468 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-linux-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-496badc04b) |
| Contract elements | 1 |
| Extent decisions | 6 |

## External contract

<a id="s-d2b0286240"></a>
- <a id="s-247fcf4997"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json
- <a id="s-b41079f681"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-78297df480"></a>`atomic_write_segments_max` | no | type="integer"; minimum=0 |  |
| <a id="s-29361b2b54"></a>`atomic_write_unit_max` | no | type="integer"; minimum=0 |  |
| <a id="s-31aff68a74"></a>`atomic_write_unit_max_opt` | no | type="integer"; minimum=0 |  |
| <a id="s-4cf686ce4f"></a>`atomic_write_unit_min` | no | type="integer"; minimum=0 |  |
| <a id="s-d52f2b56b4"></a>`blocks_512_bytes` | no | type="integer"; minimum=0 |  |
| <a id="s-eed2d9c1a0"></a>`dev_major` | no | type="integer"; minimum=0 |  |
| <a id="s-cc5a062688"></a>`dev_minor` | no | type="integer"; minimum=0 |  |
| <a id="s-1b513c4ebe"></a>`device` | yes | type="integer"; minimum=0 |  |
| <a id="s-d92cc4ff14"></a>`dio_mem_align` | no | type="integer"; minimum=0 |  |
| <a id="s-c0fcc7c143"></a>`dio_offset_align` | no | type="integer"; minimum=0 |  |
| <a id="s-3a5c8444d1"></a>`dio_read_offset_align` | no | type="integer"; minimum=0 |  |
| <a id="s-6a0982215f"></a>`gid` | yes | type="integer"; minimum=0 |  |
| <a id="s-c4b1d3826f"></a>`inode` | yes | type="integer"; minimum=0 |  |
| <a id="s-4a57f3ac85"></a>`mode` | yes | type="string"; pattern="^[0-7]+$" |  |
| <a id="s-26e68e050c"></a>`nlink` | yes | type="integer"; minimum=0 |  |
| <a id="s-3016979842"></a>`preferred_io_block_size` | no | type="integer"; minimum=0 |  |
| <a id="s-4c2c65d51c"></a>`rdev` | no | type="integer"; minimum=0 |  |
| <a id="s-c93707909a"></a>`size` | yes | type="integer"; minimum=0 |  |
| <a id="s-6e268bb003"></a>`statx_attributes` | no | type="integer"; minimum=0 |  |
| <a id="s-903d41c177"></a>`statx_attributes_mask` | no | type="integer"; minimum=0 |  |
| <a id="s-2529fd3871"></a>`statx_available` | no | type="boolean" |  |
| <a id="s-717888436d"></a>`statx_mask` | no | type="integer"; minimum=0 |  |
| <a id="s-a00e2fee21"></a>`subvolume_id` | no | type="integer"; minimum=0 |  |
| <a id="s-5ee0502fdf"></a>`uid` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field atomic_write_segments_max](#s-78297df480) | `value · schema-value · extension_owned` | shared above |
| [field blocks_512_bytes](#s-d52f2b56b4) | `value · schema-value · extension_owned` | shared above |
| [field dio_offset_align](#s-c0fcc7c143) | `value · schema-value · extension_owned` | shared above |
| [field dio_read_offset_align](#s-3a5c8444d1) | `value · schema-value · extension_owned` | shared above |
| [field preferred_io_block_size](#s-3016979842) | `value · schema-value · extension_owned` | shared above |
| [field size](#s-c93707909a) | `value · schema-value · extension_owned` | shared above |

## Governing policies

- <a id="pa-fef0f4cbec"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-70e060ff95"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json](../../../evidence/sources.md#src-79a949b157) — `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-file-stat.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1linux-file-stat.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61b29c01570ddab284846ec05646a585a9c760696ebafc145fcb9987de606321 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "atomic_write_segments_max": {
      "minimum": 0,
      "type": "integer"
    },
    "atomic_write_unit_max": {
      "minimum": 0,
      "type": "integer"
    },
    "atomic_write_unit_max_opt": {
      "minimum": 0,
      "type": "integer"
    },
    "atomic_write_unit_min": {
      "minimum": 0,
      "type": "integer"
    },
    "blocks_512_bytes": {
      "minimum": 0,
      "type": "integer"
    },
    "dev_major": {
      "minimum": 0,
      "type": "integer"
    },
    "dev_minor": {
      "minimum": 0,
      "type": "integer"
    },
    "device": {
      "minimum": 0,
      "type": "integer"
    },
    "dio_mem_align": {
      "minimum": 0,
      "type": "integer"
    },
    "dio_offset_align": {
      "minimum": 0,
      "type": "integer"
    },
    "dio_read_offset_align": {
      "minimum": 0,
      "type": "integer"
    },
    "gid": {
      "minimum": 0,
      "type": "integer"
    },
    "inode": {
      "minimum": 0,
      "type": "integer"
    },
    "mode": {
      "pattern": "^[0-7]+$",
      "type": "string"
    },
    "nlink": {
      "minimum": 0,
      "type": "integer"
    },
    "preferred_io_block_size": {
      "minimum": 0,
      "type": "integer"
    },
    "rdev": {
      "minimum": 0,
      "type": "integer"
    },
    "size": {
      "minimum": 0,
      "type": "integer"
    },
    "statx_attributes": {
      "minimum": 0,
      "type": "integer"
    },
    "statx_attributes_mask": {
      "minimum": 0,
      "type": "integer"
    },
    "statx_available": {
      "type": "boolean"
    },
    "statx_mask": {
      "minimum": 0,
      "type": "integer"
    },
    "subvolume_id": {
      "minimum": 0,
      "type": "integer"
    },
    "uid": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "device",
    "inode",
    "mode",
    "nlink",
    "uid",
    "gid",
    "size"
  ],
  "type": "object"
}
```
