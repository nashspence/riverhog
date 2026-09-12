# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-macos-contracts:https-nashspence-github-io-riverhog-v1-pr-cda5d58e04:a24d1095dd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-macos-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-24cb3a408132) |
| Contract elements | 1 |
| Extent decisions | 8 |

## External contract

<a id="s-fa8175ab36ed"></a>
- <a id="s-e76df1f7e2be"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json
- <a id="s-a98e6c7a22a1"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-37d4950c5ebe"></a>`allocation_size` | no | type="integer"; minimum=0 |  |
| <a id="s-4a6aac106d60"></a>`data_allocation_size` | no | type="integer"; minimum=0 |  |
| <a id="s-545313b1667e"></a>`data_length` | no | type="integer"; minimum=0 |  |
| <a id="s-e7cc87a4046d"></a>`document_id` | no | type="integer"; minimum=0 |  |
| <a id="s-237b7d85e372"></a>`generation` | no | type="integer"; minimum=0 |  |
| <a id="s-54c5f49a9725"></a>`io_block_size` | no | type="integer"; minimum=0 |  |
| <a id="s-8f6b576eb691"></a>`resource_fork_allocation_size` | no | type="integer"; minimum=0 |  |
| <a id="s-d63f79a6b429"></a>`resource_fork_length` | no | type="integer"; minimum=0 |  |
| <a id="s-170283d8ae58"></a>`total_size` | no | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json](#s-fa8175ab36ed) | `cardinality · entries · extension_owned` | shared above |
| [field allocation_size](#s-37d4950c5ebe) | `value · schema-value · extension_owned` | shared above |
| [field data_allocation_size](#s-4a6aac106d60) | `value · schema-value · extension_owned` | shared above |
| [field data_length](#s-545313b1667e) | `value · schema-value · extension_owned` | shared above |
| [field io_block_size](#s-54c5f49a9725) | `value · schema-value · extension_owned` | shared above |
| [field resource_fork_allocation_size](#s-8f6b576eb691) | `value · schema-value · extension_owned` | shared above |
| [field resource_fork_length](#s-d63f79a6b429) | `value · schema-value · extension_owned` | shared above |
| [field total_size](#s-170283d8ae58) | `value · schema-value · extension_owned` | shared above |

## Governing policies

- <a id="pa-f92e0a714f33"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-0e6ebb308dea"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json](../../../evidence/sources.md#src-8191cd2baf94) — `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/darwin-file-attributes.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1darwin-file-attributes.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d93ccd741dbce58c24079da050ac8396123467f74d0e749607ce56e379106d5c -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "minProperties": 1,
  "properties": {
    "allocation_size": {
      "minimum": 0,
      "type": "integer"
    },
    "data_allocation_size": {
      "minimum": 0,
      "type": "integer"
    },
    "data_length": {
      "minimum": 0,
      "type": "integer"
    },
    "document_id": {
      "minimum": 0,
      "type": "integer"
    },
    "generation": {
      "minimum": 0,
      "type": "integer"
    },
    "io_block_size": {
      "minimum": 0,
      "type": "integer"
    },
    "resource_fork_allocation_size": {
      "minimum": 0,
      "type": "integer"
    },
    "resource_fork_length": {
      "minimum": 0,
      "type": "integer"
    },
    "total_size": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "type": "object"
}
```
