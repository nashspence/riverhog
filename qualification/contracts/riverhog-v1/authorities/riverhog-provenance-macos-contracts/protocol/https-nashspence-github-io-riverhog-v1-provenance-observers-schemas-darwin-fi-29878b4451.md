# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-macos-contracts:https-nashspence-github-io-riverhog-v1-pr-29878b4451:2f76ebda03 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-macos-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-24cb3a4081) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-d0bea0340f"></a>
- <a id="s-60cfd18f01"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json
- <a id="s-efd0fa22d6"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b619b5f7ca"></a>`blocks_512_bytes` | no | type="integer"; minimum=0 |  |
| <a id="s-abf2fedaa3"></a>`device` | yes | type="integer"; minimum=0 |  |
| <a id="s-3010a7cb62"></a>`flags` | no | type="integer"; minimum=0 |  |
| <a id="s-3ce78dfebe"></a>`generation` | no | type="integer"; minimum=0 |  |
| <a id="s-46187dc335"></a>`gid` | yes | type="integer"; minimum=0 |  |
| <a id="s-d460520d95"></a>`inode` | yes | type="integer"; minimum=0 |  |
| <a id="s-90072bb791"></a>`mode` | yes | type="string"; pattern="^[0-7]+$" |  |
| <a id="s-5aec627e87"></a>`nlink` | yes | type="integer"; minimum=0 |  |
| <a id="s-dbb8144c35"></a>`preferred_io_block_size` | no | type="integer"; minimum=0 |  |
| <a id="s-ab23803f6c"></a>`rdev` | no | type="integer"; minimum=0 |  |
| <a id="s-997352bd00"></a>`size` | yes | type="integer"; minimum=0 |  |
| <a id="s-3adabad44a"></a>`uid` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field blocks_512_bytes](#s-b619b5f7ca) | `value · schema-value · extension_owned` | shared above |
| [field preferred_io_block_size](#s-dbb8144c35) | `value · schema-value · extension_owned` | shared above |
| [field size](#s-997352bd00) | `value · schema-value · extension_owned` | shared above |

## Governing policies

- <a id="pa-a345821e67"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-4fda5b36db"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json](../../../evidence/sources.md#src-7295b2b671) — `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/darwin-file-stat.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1darwin-file-stat.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a9731ecced70d231000fe7771ba7028735379cf2b56a77864b38177c51409de9 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "blocks_512_bytes": {
      "minimum": 0,
      "type": "integer"
    },
    "device": {
      "minimum": 0,
      "type": "integer"
    },
    "flags": {
      "minimum": 0,
      "type": "integer"
    },
    "generation": {
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
