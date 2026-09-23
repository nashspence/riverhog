# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-provenance-linux-contracts:https-nashspence-github-io-riverhog-v1-pr-ce7cee8b8b:65b8ec1f20 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-linux-contracts](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-f19000daa2"></a>

- <a id="s-d9da4f992c"></a>`type`: `"object"`
- <a id="s-22cc58575a"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json"`
- <a id="s-be57f3c4fd"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-52ed411979"></a>`additionalProperties`: `false`
- <a id="s-a2099f814c"></a>`required`: `["xflags","xflag_names","extent_size","nextents","project_id","cow_extent_size"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6de7ca957c"></a>`cow_extent_size` | yes | type="integer"; minimum=0 |  |
| <a id="s-4f887e8570"></a>`extent_size` | yes | type="integer"; minimum=0 |  |
| <a id="s-dc83d4064f"></a>`nextents` | yes | type="integer"; minimum=0 |  |
| <a id="s-bde51c8c0f"></a>`project_id` | yes | type="integer"; minimum=0 |  |
| <a id="s-5aa7e4b9c0"></a>`xflag_names` | yes | type="array"; items=(type="string"); uniqueItems=true |  |
| <a id="s-3f6fc5521c"></a>`xflags` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/extension-contract/v1](../../extent-contract/extent/extent-rule-extension-contract.md#p-75a89f9d1c)

Shared facts for every subject below: maximum=null; reason="independently-versioned-extension-authority"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field cow_extent_size](#s-6de7ca957c) | `value · schema-value · extension_owned` | shared above |
| [field extent_size](#s-4f887e8570) | `value · schema-value · extension_owned` | shared above |
| [field xflag_names](#s-5aa7e4b9c0) | `cardinality · items · extension_owned` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f10c15a944"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-1239d7bb8d"></a>[extent-rule/extension-contract/v1](../../extent-contract/extent/extent-rule-extension-contract.md#p-75a89f9d1c)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json](../../../evidence/sources/authorities.md#src-8ed956c86a) — [reference/riverhog/provenance/contracts/linux/src/riverhog\_provenance\_linux\_contracts/schemas/linux-fsxattr.schema.json](../../../../../../reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-fsxattr.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1linux-fsxattr.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa34df9dec8d9b0062148907b6cd8645a40bd941ffa212387bcbd09f922747b1 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "cow_extent_size": {
      "minimum": 0,
      "type": "integer"
    },
    "extent_size": {
      "minimum": 0,
      "type": "integer"
    },
    "nextents": {
      "minimum": 0,
      "type": "integer"
    },
    "project_id": {
      "minimum": 0,
      "type": "integer"
    },
    "xflag_names": {
      "items": {
        "type": "string"
      },
      "type": "array",
      "uniqueItems": true
    },
    "xflags": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "xflags",
    "xflag_names",
    "extent_size",
    "nextents",
    "project_id",
    "cow_extent_size"
  ],
  "type": "object"
}
```

</details>
