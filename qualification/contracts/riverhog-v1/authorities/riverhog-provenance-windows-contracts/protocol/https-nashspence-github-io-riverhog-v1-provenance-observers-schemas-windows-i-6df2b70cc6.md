# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-6df2b70cc6:a12e386383 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-78a2bea4a4b2) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-11492b1a1dfc"></a>
- <a id="s-2e25bc508db4"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json
- <a id="s-8f43ce666879"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6b34391f14fa"></a>`checksum_algorithm` | yes | type="integer"; minimum=0 |  |
| <a id="s-7cc3ef92f463"></a>`checksum_chunk_size` | yes | type="integer"; minimum=0 |  |
| <a id="s-aa06b991febf"></a>`cluster_size` | yes | type="integer"; minimum=0 |  |
| <a id="s-6f8c66aa47e3"></a>`flags` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field checksum_chunk_size](#s-7cc3ef92f463) | `value · schema-value · extension_owned` | shared above |
| [field cluster_size](#s-aa06b991febf) | `value · schema-value · extension_owned` | shared above |

## Governing policies

- <a id="pa-b57042857d39"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-f22f39b0cce0"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json](../../../evidence/sources.md#src-c4a2d3a89289) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-integrity-info.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-integrity-info.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 986c12768cb14d0aceae4c926c74f7230aac4e51cd9cbb0c04e6453b35a2aaad -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "checksum_algorithm": {
      "minimum": 0,
      "type": "integer"
    },
    "checksum_chunk_size": {
      "minimum": 0,
      "type": "integer"
    },
    "cluster_size": {
      "minimum": 0,
      "type": "integer"
    },
    "flags": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "checksum_algorithm",
    "flags",
    "checksum_chunk_size",
    "cluster_size"
  ],
  "type": "object"
}
```
