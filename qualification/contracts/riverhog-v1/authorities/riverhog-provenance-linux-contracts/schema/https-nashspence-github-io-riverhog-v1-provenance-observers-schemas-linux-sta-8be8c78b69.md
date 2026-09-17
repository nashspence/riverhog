# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-provenance-linux-contracts:https-nashspence-github-io-riverhog-v1-pr-8be8c78b69:3369e8ce69 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-linux-contracts](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-f7d1eefa0b"></a>

- <a id="s-fa0152c200"></a>`type`: `"object"`
- <a id="s-561194c537"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json"`
- <a id="s-8bc3a60c98"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-05bc18e74c"></a>`additionalProperties`: `false`
- <a id="s-6c19c8fdac"></a>`required`: `["attributes","attributes_mask","set_names","supported_names"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-906b1a3271"></a>`attributes` | yes | type="integer"; minimum=0 |  |
| <a id="s-1a149c51de"></a>`attributes_mask` | yes | type="integer"; minimum=0 |  |
| <a id="s-d309266269"></a>`set_names` | yes | type="array"; items=(type="string"); uniqueItems=true |  |
| <a id="s-d7a5a01918"></a>`supported_names` | yes | type="array"; items=(type="string"); uniqueItems=true |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field set_names](#s-d309266269) | `cardinality · items · extension_owned` | shared above |
| [field supported_names](#s-d7a5a01918) | `cardinality · items · extension_owned` | shared above |

## Governing policies

- <a id="pa-61d0cedd76"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-6190a9c5dc"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json](../../../evidence/sources.md#src-ea93e415ba) — [reference/riverhog/provenance/contracts/linux/src/riverhog\_provenance\_linux\_contracts/schemas/linux-statx-attributes.schema.json](../../../../../../reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-statx-attributes.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1linux-statx-attributes.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56fc562c3d49ff461fc7c95a24a26dc1c57814bd8fce8bbaec9ceb2eff4f9351 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "attributes": {
      "minimum": 0,
      "type": "integer"
    },
    "attributes_mask": {
      "minimum": 0,
      "type": "integer"
    },
    "set_names": {
      "items": {
        "type": "string"
      },
      "type": "array",
      "uniqueItems": true
    },
    "supported_names": {
      "items": {
        "type": "string"
      },
      "type": "array",
      "uniqueItems": true
    }
  },
  "required": [
    "attributes",
    "attributes_mask",
    "set_names",
    "supported_names"
  ],
  "type": "object"
}
```

</details>
