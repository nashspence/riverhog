# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-2f54ac7817:f0769b70a0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-7513fa2e7c"></a>

- <a id="s-f2bbe7939f"></a>`type`: `"object"`
- <a id="s-6994147f35"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json"`
- <a id="s-ac9c805477"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-6db6f21867"></a>`additionalProperties`: `false`
- <a id="s-0fc2fe8fcf"></a>`required`: `["bitmask","hex","names","reparse_tag"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-50e5a25871"></a>`bitmask` | yes | type="integer"; minimum=0 |  |
| <a id="s-92b94d461f"></a>`hex` | yes | type="string"; pattern="^0x[0-9a-f]{8}$" |  |
| <a id="s-1bf4b3809a"></a>`names` | yes | type="array"; items=(type="string"; minLength=1); uniqueItems=true |  |
| <a id="s-65fcdf5afe"></a>`reparse_tag` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field names](#s-1bf4b3809a) | `cardinality · items · extension_owned` | shared above |

## Governing policies

- <a id="pa-bd68e26f05"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-103eb58ea5"></a>[extent-rule/extension-contract/v1](../../../policies/index.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json](../../../evidence/sources.md#src-01ce873e85) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-file-attributes.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-file-attributes.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37bba6822376342893fc3815b03e3da54ba3828bdbeb7cfe4fa6cd763a3055d4 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "bitmask": {
      "minimum": 0,
      "type": "integer"
    },
    "hex": {
      "pattern": "^0x[0-9a-f]{8}$",
      "type": "string"
    },
    "names": {
      "items": {
        "minLength": 1,
        "type": "string"
      },
      "type": "array",
      "uniqueItems": true
    },
    "reparse_tag": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "bitmask",
    "hex",
    "names",
    "reparse_tag"
  ],
  "type": "object"
}
```

</details>
