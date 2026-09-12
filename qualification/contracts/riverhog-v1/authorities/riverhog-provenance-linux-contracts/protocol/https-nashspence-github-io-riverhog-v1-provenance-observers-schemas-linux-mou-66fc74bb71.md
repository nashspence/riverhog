# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-linux-contracts:https-nashspence-github-io-riverhog-v1-pr-66fc74bb71:2727c47cb7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-linux-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-496badc04bf0) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-0ad7283c6e8a"></a>
- <a id="s-c27f03c13617"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json
- <a id="s-f419b07e1dcd"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-54e361a1926d"></a>`device` | yes | type="string" |  |
| <a id="s-31b1481175fc"></a>`filesystem_type` | yes | type="string"; minLength=1 |  |
| <a id="s-12eb9be9f1fb"></a>`mount_id` | yes | type="integer"; minimum=0 |  |
| <a id="s-5505318b9a07"></a>`mount_options` | yes | type="array"; items=(type="string") |  |
| <a id="s-82d6e70db3c0"></a>`mount_point` | yes | type="string" |  |
| <a id="s-9608e7167410"></a>`optional_fields` | yes | type="array"; items=(type="string") |  |
| <a id="s-54727ec58e50"></a>`parent_id` | yes | type="integer"; minimum=0 |  |
| <a id="s-3a2f3828a029"></a>`root` | yes | type="string" |  |
| <a id="s-151d09400788"></a>`source` | yes | type="string" |  |
| <a id="s-8301711ff55c"></a>`super_options` | yes | type="array"; items=(type="string") |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field mount_options](#s-5505318b9a07) | `cardinality · items · extension_owned` | shared above |
| [field optional_fields](#s-9608e7167410) | `cardinality · items · extension_owned` | shared above |
| [field super_options](#s-8301711ff55c) | `cardinality · items · extension_owned` | shared above |

## Governing policies

- <a id="pa-bd9dac78e743"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-ba56e94041d0"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json](../../../evidence/sources.md#src-e3afb2680c06) — `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-mount-context.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1linux-mount-context.json`

### Exact owned JSON

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
