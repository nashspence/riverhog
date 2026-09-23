# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:a-riverhog-windows-provenance-contract-lib:https-nashspence-github-io-riverhog-v1-pr-6df2b70cc6:676a8beb9f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-windows-provenance-contract-lib](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-11492b1a1d"></a>

- <a id="s-8f43ce6668"></a>`type`: `"object"`
- <a id="s-2e25bc508d"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json"`
- <a id="s-10e3a3c299"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-f8fcaa3e26"></a>`additionalProperties`: `false`
- <a id="s-bdbebb8a8a"></a>`required`: `["checksum_algorithm","flags","checksum_chunk_size","cluster_size"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6b34391f14"></a>`checksum_algorithm` | yes | type="integer"; minimum=0 |  |
| <a id="s-7cc3ef92f4"></a>`checksum_chunk_size` | yes | type="integer"; minimum=0 |  |
| <a id="s-aa06b991fe"></a>`cluster_size` | yes | type="integer"; minimum=0 |  |
| <a id="s-6f8c66aa47"></a>`flags` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../extent-contract/extent/extent-rule-extension-contract.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field checksum_chunk_size](#s-7cc3ef92f4) | `value · schema-value · extension_owned` | shared above |
| [field cluster_size](#s-aa06b991fe) | `value · schema-value · extension_owned` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e2b853c070"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-b89a0b632c"></a>[extent-rule/extension-contract/v1](../../extent-contract/extent/extent-rule-extension-contract.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json](../../../evidence/sources/authorities.md#src-c4a2d3a892) — [some-implementations/riverhog/provenance/contracts/windows/src/a\_riverhog\_windows\_provenance\_contract\_lib/schemas/windows-integrity-info.schema.json](../../../../../../some-implementations/riverhog/provenance/contracts/windows/src/a_riverhog_windows_provenance_contract_lib/schemas/windows-integrity-info.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-integrity-info.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
