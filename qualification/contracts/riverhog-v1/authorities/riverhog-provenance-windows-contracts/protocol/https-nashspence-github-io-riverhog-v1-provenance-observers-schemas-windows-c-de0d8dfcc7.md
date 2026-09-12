# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-de0d8dfcc7:68551531bd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-78a2bea4a4) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-13769e3ceb"></a>
- <a id="s-a06b3f827b"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json
- <a id="s-3d137e91b3"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-06ce68c795"></a>`chunk_shift` | yes | type="integer"; minimum=0 |  |
| <a id="s-f191a3a820"></a>`cluster_shift` | yes | type="integer"; minimum=0 |  |
| <a id="s-41996ac622"></a>`compressed_size` | yes | type="integer"; minimum=0 |  |
| <a id="s-08de6a3103"></a>`compression_format` | yes | type="integer"; minimum=0 |  |
| <a id="s-4bd5e7b2fd"></a>`compression_format_name` | yes | type="string"; minLength=1 |  |
| <a id="s-1e32f82e2f"></a>`compression_unit_shift` | yes | type="integer"; minimum=0 |  |
| <a id="s-878f1ca9bf"></a>`file_attribute_compressed` | yes | type="boolean" |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field compressed_size](#s-41996ac622) | `value · schema-value · extension_owned` | shared above |

## Governing policies

- <a id="pa-aa1b810e5e"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-b04eabd3d0"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json](../../../evidence/sources.md#src-40e7549720) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-compression-state.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-compression-state.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 136807e5ab3720e22873df3b3b79f28d20fb61114088a774f4238bb55903c169 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "chunk_shift": {
      "minimum": 0,
      "type": "integer"
    },
    "cluster_shift": {
      "minimum": 0,
      "type": "integer"
    },
    "compressed_size": {
      "minimum": 0,
      "type": "integer"
    },
    "compression_format": {
      "minimum": 0,
      "type": "integer"
    },
    "compression_format_name": {
      "minLength": 1,
      "type": "string"
    },
    "compression_unit_shift": {
      "minimum": 0,
      "type": "integer"
    },
    "file_attribute_compressed": {
      "type": "boolean"
    }
  },
  "required": [
    "file_attribute_compressed",
    "compressed_size",
    "compression_format",
    "compression_format_name",
    "compression_unit_shift",
    "chunk_shift",
    "cluster_shift"
  ],
  "type": "object"
}
```
