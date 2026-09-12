# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-macos-contracts:https-nashspence-github-io-riverhog-v1-pr-3861688670:4b8a8e38fc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-macos-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-24cb3a4081) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-4af24baace"></a>
- <a id="s-9be392290a"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json
- <a id="s-745234cda4"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7dd8ca929b"></a>`raw` | yes | type="integer"; minimum=0 |  |
| <a id="s-b74eae877a"></a>`set_names` | yes | type="array"; items=(type="string"); additional keys=`uniqueItems` |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field set_names](#s-b74eae877a) | `cardinality · items · extension_owned` | shared above |

## Governing policies

- <a id="pa-348958def4"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-0d966e2b1f"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json](../../../evidence/sources.md#src-cb9d75fa32) — `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/darwin-file-flags.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1darwin-file-flags.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f22a6bd3462e45de8aecc3e1955bf8e092460c78fbaa84e9aec700b5632800eb -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "raw": {
      "minimum": 0,
      "type": "integer"
    },
    "set_names": {
      "items": {
        "type": "string"
      },
      "type": "array",
      "uniqueItems": true
    }
  },
  "required": [
    "raw",
    "set_names"
  ],
  "type": "object"
}
```
