# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-provenance-linux-contracts:https-nashspence-github-io-riverhog-v1-pr-66fc74bb71:92ace7ba71 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-linux-contracts](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-0ad7283c6e"></a>

- <a id="s-f419b07e1d"></a>`type`: `"object"`
- <a id="s-c27f03c136"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json"`
- <a id="s-398b9a9735"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-2549788df5"></a>`additionalProperties`: `false`
- <a id="s-26929e00ae"></a>`required`: `["mount_id","parent_id","device","root","mount_point","mount_options","optional_fields","filesystem_type","source","super_options"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-54e361a192"></a>`device` | yes | type="string" |  |
| <a id="s-31b1481175"></a>`filesystem_type` | yes | type="string"; minLength=1 |  |
| <a id="s-12eb9be9f1"></a>`mount_id` | yes | type="integer"; minimum=0 |  |
| <a id="s-5505318b9a"></a>`mount_options` | yes | type="array"; items=(type="string") |  |
| <a id="s-82d6e70db3"></a>`mount_point` | yes | type="string" |  |
| <a id="s-9608e71674"></a>`optional_fields` | yes | type="array"; items=(type="string") |  |
| <a id="s-54727ec58e"></a>`parent_id` | yes | type="integer"; minimum=0 |  |
| <a id="s-3a2f3828a0"></a>`root` | yes | type="string" |  |
| <a id="s-151d094007"></a>`source` | yes | type="string" |  |
| <a id="s-8301711ff5"></a>`super_options` | yes | type="array"; items=(type="string") |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../extent-contract/extent/extent-rule-extension-contract.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field mount_options](#s-5505318b9a) | `cardinality · items · extension_owned` | shared above |
| [field optional_fields](#s-9608e71674) | `cardinality · items · extension_owned` | shared above |
| [field super_options](#s-8301711ff5) | `cardinality · items · extension_owned` | shared above |

## Governing policies

- <a id="pa-954c13c870"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-a63ab2db41"></a>[extent-rule/extension-contract/v1](../../extent-contract/extent/extent-rule-extension-contract.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json](../../../evidence/sources/authorities.md#src-e3afb2680c) — [reference/riverhog/provenance/contracts/linux/src/riverhog\_provenance\_linux\_contracts/schemas/linux-mount-context.schema.json](../../../../../../reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-mount-context.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1linux-mount-context.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0443a380bfbe8e785650c1ee81ec9e13f188682a1e33f80dc7e4df05a087bc0 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "device": {
      "type": "string"
    },
    "filesystem_type": {
      "minLength": 1,
      "type": "string"
    },
    "mount_id": {
      "minimum": 0,
      "type": "integer"
    },
    "mount_options": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "mount_point": {
      "type": "string"
    },
    "optional_fields": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "parent_id": {
      "minimum": 0,
      "type": "integer"
    },
    "root": {
      "type": "string"
    },
    "source": {
      "type": "string"
    },
    "super_options": {
      "items": {
        "type": "string"
      },
      "type": "array"
    }
  },
  "required": [
    "mount_id",
    "parent_id",
    "device",
    "root",
    "mount_point",
    "mount_options",
    "optional_fields",
    "filesystem_type",
    "source",
    "super_options"
  ],
  "type": "object"
}
```

</details>
