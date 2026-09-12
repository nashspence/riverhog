# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-linux-contracts:https-nashspence-github-io-riverhog-v1-pr-8be8c78b69:0fe7178446 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-linux-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-496badc04bf0) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-f7d1eefa0bc5"></a>
- <a id="s-561194c53709"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json
- <a id="s-fa0152c20058"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-906b1a32718c"></a>`attributes` | yes | type="integer"; minimum=0 |  |
| <a id="s-1a149c51de2a"></a>`attributes_mask` | yes | type="integer"; minimum=0 |  |
| <a id="s-d30926626919"></a>`set_names` | yes | type="array"; items=(type="string"); additional keys=`uniqueItems` |  |
| <a id="s-d7a5a019181e"></a>`supported_names` | yes | type="array"; items=(type="string"); additional keys=`uniqueItems` |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field set_names](#s-d30926626919) | `cardinality · items · extension_owned` | shared above |
| [field supported_names](#s-d7a5a019181e) | `cardinality · items · extension_owned` | shared above |

## Governing policies

- <a id="pa-1104466d02d4"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-4cbe29bb68a2"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json](../../../evidence/sources.md#src-ea93e415ba1d) — `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-statx-attributes.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1linux-statx-attributes.json`

### Exact owned JSON

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
