# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-macos-contracts:https-nashspence-github-io-riverhog-v1-pr-6c8c337f8e:aa33cf5f83 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-macos-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-24cb3a408132) |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

<a id="s-f66e0e33bd89"></a>
- <a id="s-c4c1b282c321"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json
- <a id="s-636512cba77b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f2e32e479705"></a>`block_size` | yes | type="integer"; minimum=0 |  |
| <a id="s-43114201cf4f"></a>`capabilities` | no | type="array"; items=(type="integer"; minimum=0) |  |
| <a id="s-4446990b9faf"></a>`filesystem_subtype` | yes | type="integer"; minimum=0 |  |
| <a id="s-260fe4189774"></a>`filesystem_type` | yes | type="string"; minLength=1 |  |
| <a id="s-614684236b14"></a>`fsid` | yes | type="array"; items=(false); additional keys=`prefixItems` |  |
| <a id="s-ee3db8e92a64"></a>`io_size` | yes | type="integer"; minimum=0 |  |
| <a id="s-863f4ce4e5cd"></a>`mount_flags` | yes | type="integer"; minimum=0 |  |
| <a id="s-b8a066ca6eba"></a>`mount_point` | yes | type="string" |  |
| <a id="s-2f1c9badc592"></a>`mounted_from` | yes | type="string" |  |
| <a id="s-d06da5ba6ad2"></a>`valid_capabilities` | no | type="array"; items=(type="integer"; minimum=0) |  |
| <a id="s-daf9c9b338af"></a>`volume_uuid` | no | type="string"; format="uuid"; pattern="^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field block_size](#s-f2e32e479705) | `value · schema-value · extension_owned` | shared above |
| <a id="s-ccf86f48c1d1"></a>field capabilities · items | `value · schema-value · extension_owned` | shared above |
| [field capabilities](#s-43114201cf4f) | `cardinality · items · extension_owned` | shared above |
| [field fsid](#s-614684236b14) | `cardinality · items · extension_owned` | shared above |
| [field io_size](#s-ee3db8e92a64) | `value · schema-value · extension_owned` | shared above |
| <a id="s-ae7985e51c9a"></a>field valid_capabilities · items | `value · schema-value · extension_owned` | shared above |
| [field valid_capabilities](#s-d06da5ba6ad2) | `cardinality · items · extension_owned` | shared above |

## Governing policies

- <a id="pa-fa104c3d99ec"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-3e92180b807b"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json](../../../evidence/sources.md#src-86983ef4106a) — `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/macos-volume-context.schema.json`

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
