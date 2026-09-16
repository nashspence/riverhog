# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-780129ba3c:3f0331671c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-e4a7d24cfa"></a>

- <a id="s-2387fdeb5d"></a>`type`: `"object"`
- <a id="s-d07a61acfb"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json"`
- <a id="s-7ce45c6035"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-dd182b084e"></a>`additionalProperties`: `false`
- <a id="s-3527c8eb50"></a>`required`: `["stream_id","stream_attributes","stream_attribute_names","stream_size"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f79f667806"></a>`ea_flags` | no | type="integer"; minimum=0 |  |
| <a id="s-23d758e7ec"></a>`need_ea` | no | type="boolean" |  |
| <a id="s-a7cdaeb25f"></a>`stream_attribute_names` | yes | type="array"; items=(type="string"; minLength=1); uniqueItems=true |  |
| <a id="s-0abf5d8030"></a>`stream_attributes` | yes | type="integer"; minimum=0 |  |
| <a id="s-8bc3a2a840"></a>`stream_id` | yes | type="integer"; minimum=0 |  |
| <a id="s-92c1562d72"></a>`stream_size` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field stream_attribute_names](#s-a7cdaeb25f) | `cardinality · items · extension_owned` | shared above |
| [field stream_size](#s-92c1562d72) | `value · schema-value · extension_owned` | shared above |

## Governing policies

- <a id="pa-ce9f92b453"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-90b6052613"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json](../../../evidence/sources.md#src-40a5d23088) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-backup-stream-info.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-backup-stream-info.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
