# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-780129ba3c:680fb2d9ea -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-78a2bea4a4b2) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-e4a7d24cfaaa"></a>
- <a id="s-d07a61acfb4b"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json
- <a id="s-2387fdeb5d50"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f79f66780647"></a>`ea_flags` | no | type="integer"; minimum=0 |  |
| <a id="s-23d758e7ec6b"></a>`need_ea` | no | type="boolean" |  |
| <a id="s-a7cdaeb25f39"></a>`stream_attribute_names` | yes | type="array"; items=(type="string"; minLength=1); additional keys=`uniqueItems` |  |
| <a id="s-0abf5d8030f1"></a>`stream_attributes` | yes | type="integer"; minimum=0 |  |
| <a id="s-8bc3a2a84070"></a>`stream_id` | yes | type="integer"; minimum=0 |  |
| <a id="s-92c1562d7287"></a>`stream_size` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field stream_attribute_names](#s-a7cdaeb25f39) | `cardinality · items · extension_owned` | shared above |
| [field stream_size](#s-92c1562d7287) | `value · schema-value · extension_owned` | shared above |

## Governing policies

- <a id="pa-19954ff6b243"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-44b26a566c70"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c36)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json](../../../evidence/sources.md#src-40a5d2308830) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-backup-stream-info.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-backup-stream-info.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5749ab8c4944e69d57543fc32f3be4ed3f02eb66c903884166f368a98a0e4b44 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "ea_flags": {
      "minimum": 0,
      "type": "integer"
    },
    "need_ea": {
      "type": "boolean"
    },
    "stream_attribute_names": {
      "items": {
        "minLength": 1,
        "type": "string"
      },
      "type": "array",
      "uniqueItems": true
    },
    "stream_attributes": {
      "minimum": 0,
      "type": "integer"
    },
    "stream_id": {
      "minimum": 0,
      "type": "integer"
    },
    "stream_size": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "stream_id",
    "stream_attributes",
    "stream_attribute_names",
    "stream_size"
  ],
  "type": "object"
}
```
