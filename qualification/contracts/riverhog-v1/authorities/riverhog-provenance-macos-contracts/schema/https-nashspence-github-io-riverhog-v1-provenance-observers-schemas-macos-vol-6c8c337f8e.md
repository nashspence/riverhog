# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-provenance-macos-contracts:https-nashspence-github-io-riverhog-v1-pr-6c8c337f8e:bb7dcbe4f7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-macos-contracts](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-f66e0e33bd"></a>

- <a id="s-636512cba7"></a>`type`: `"object"`
- <a id="s-c4c1b282c3"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json"`
- <a id="s-a1eaf01af8"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-70518728f5"></a>`additionalProperties`: `false`
- <a id="s-beb5a2e12b"></a>`required`: `["filesystem_type","mount_point","mounted_from","fsid","mount_flags","filesystem_subtype","io_size","block_size"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f2e32e4797"></a>`block_size` | yes | type="integer"; minimum=0 |  |
| <a id="s-43114201cf"></a>`capabilities` | no | type="array"; items=(type="integer"; minimum=0) |  |
| <a id="s-4446990b9f"></a>`filesystem_subtype` | yes | type="integer"; minimum=0 |  |
| <a id="s-260fe41897"></a>`filesystem_type` | yes | type="string"; minLength=1 |  |
| `fsid` | yes | [See field `fsid`](#s-614684236b) |  |
| <a id="s-ee3db8e92a"></a>`io_size` | yes | type="integer"; minimum=0 |  |
| <a id="s-863f4ce4e5"></a>`mount_flags` | yes | type="integer"; minimum=0 |  |
| <a id="s-b8a066ca6e"></a>`mount_point` | yes | type="string" |  |
| <a id="s-2f1c9badc5"></a>`mounted_from` | yes | type="string" |  |
| <a id="s-d06da5ba6a"></a>`valid_capabilities` | no | type="array"; items=(type="integer"; minimum=0) |  |
| <a id="s-daf9c9b338"></a>`volume_uuid` | no | type="string"; format="uuid"; pattern="^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$" |  |

### <a id="s-614684236b"></a>field `fsid`

- <a id="s-d668d36d7a"></a>`type`: `"array"`
- <a id="s-3cbf413b3b"></a>`items`: `false`

#### Positional item schemas, in order (`prefixItems`)

| Alternative | Schema |
|---|---|
| <a id="s-e1220bfa5e"></a>1 | type="integer" |
| <a id="s-32b57d1937"></a>2 | type="integer" |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field block_size](#s-f2e32e4797) | `value · schema-value · extension_owned` | shared above |
| <a id="s-ccf86f48c1"></a>[field capabilities · items](#s-43114201cf) | `value · schema-value · extension_owned` | shared above |
| [field capabilities](#s-43114201cf) | `cardinality · items · extension_owned` | shared above |
| [field fsid](#s-614684236b) | `cardinality · items · extension_owned` | shared above |
| [field io_size](#s-ee3db8e92a) | `value · schema-value · extension_owned` | shared above |
| <a id="s-ae7985e51c"></a>[field valid_capabilities · items](#s-d06da5ba6a) | `value · schema-value · extension_owned` | shared above |
| [field valid_capabilities](#s-d06da5ba6a) | `cardinality · items · extension_owned` | shared above |

## Governing policies

- <a id="pa-ca7b406f46"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-f9db826cb8"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json](../../../evidence/sources.md#src-86983ef410) — `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/macos-volume-context.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1macos-volume-context.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
