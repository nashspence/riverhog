# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:a-riverhog-macos-provenance-contract-lib:https-nashspence-github-io-riverhog-v1-pr-3861688670:eae72836a9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-macos-provenance-contract-lib](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-4af24baace"></a>

- <a id="s-745234cda4"></a>`type`: `"object"`
- <a id="s-9be392290a"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json"`
- <a id="s-9c07e79c52"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-94feb1976f"></a>`additionalProperties`: `false`
- <a id="s-fd659b8bfd"></a>`required`: `["raw","set_names"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7dd8ca929b"></a>`raw` | yes | type="integer"; minimum=0 |  |
| <a id="s-b74eae877a"></a>`set_names` | yes | type="array"; items=(type="string"); uniqueItems=true |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../extent-contract/extent/extent-rule-extension-contract.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field set_names](#s-b74eae877a) | `cardinality · items · extension_owned` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8635097f43"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-a8ed2c5eae"></a>[extent-rule/extension-contract/v1](../../extent-contract/extent/extent-rule-extension-contract.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json](../../../evidence/sources/authorities.md#src-cb9d75fa32) — [some-implementations/riverhog/provenance/contracts/macos/src/a\_riverhog\_macos\_provenance\_contract\_lib/schemas/darwin-file-flags.schema.json](../../../../../../some-implementations/riverhog/provenance/contracts/macos/src/a_riverhog_macos_provenance_contract_lib/schemas/darwin-file-flags.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1darwin-file-flags.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
