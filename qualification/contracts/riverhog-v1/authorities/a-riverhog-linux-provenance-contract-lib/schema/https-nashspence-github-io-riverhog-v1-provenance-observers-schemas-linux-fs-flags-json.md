# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:a-riverhog-linux-provenance-contract-lib:https-nashspence-github-io-riverhog-v1-pr-0cf0b9725d:8ca1f97dcf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-linux-provenance-contract-lib](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-484cc6c2c6"></a>

- <a id="s-a877f59d9b"></a>`type`: `"object"`
- <a id="s-cc4fe08925"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json"`
- <a id="s-b22043d543"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-fe4dd186cb"></a>`additionalProperties`: `false`
- <a id="s-a7647565b7"></a>`required`: `["raw","set_names"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2b9e4ff160"></a>`raw` | yes | type="integer"; minimum=0 |  |
| <a id="s-1bb5c0f380"></a>`set_names` | yes | type="array"; items=(type="string"); uniqueItems=true |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../extent-contract/extent/extent-rule-extension-contract.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field set_names](#s-1bb5c0f380) | `cardinality · items · extension_owned` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-902d064440"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-2f86d2270b"></a>[extent-rule/extension-contract/v1](../../extent-contract/extent/extent-rule-extension-contract.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json](../../../evidence/sources/authorities.md#src-5f8f37c8a2) — [some-implementations/riverhog/provenance/contracts/linux/src/a\_riverhog\_linux\_provenance\_contract\_lib/schemas/linux-fs-flags.schema.json](../../../../../../some-implementations/riverhog/provenance/contracts/linux/src/a_riverhog_linux_provenance_contract_lib/schemas/linux-fs-flags.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1linux-fs-flags.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: db4e0172d68ed4679f01a318962ea89f52c0ae405d53335fc21458b7b9cf74ec -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json",
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
