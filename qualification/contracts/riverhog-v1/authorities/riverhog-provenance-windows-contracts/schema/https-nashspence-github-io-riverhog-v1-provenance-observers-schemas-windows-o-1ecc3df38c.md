# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-1ecc3df38c:a7a6311695 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-bf0784e7cb"></a>

- <a id="s-e53f3faedf"></a>`type`: `"object"`
- <a id="s-bc6e6a1ac7"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json"`
- <a id="s-e94a056d77"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-dd1b746d10"></a>`additionalProperties`: `false`
- <a id="s-605c78eca8"></a>`required`: `["object_id","extended_info"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0f71aad861"></a>`extended_info` | yes | type="string"; pattern="^[0-9a-f]{0,96}$" |  |
| <a id="s-660bc1b26b"></a>`object_id` | yes | type="string"; pattern="^[0-9a-f]{32}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=32; minimum=32; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{32}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field object_id](#s-660bc1b26b) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-e23f9a7b0f"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-0d92a2c1e2"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json](../../../evidence/sources/authorities.md#src-76f06f0f43) — [reference/riverhog/provenance/contracts/windows/src/riverhog\_provenance\_windows\_contracts/schemas/windows-object-id.schema.json](../../../../../../reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-object-id.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-object-id.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 430a561b65a9b122a328d3b778f6554d4595b652086d22adef84174b8d892a00 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "extended_info": {
      "pattern": "^[0-9a-f]{0,96}$",
      "type": "string"
    },
    "object_id": {
      "pattern": "^[0-9a-f]{32}$",
      "type": "string"
    }
  },
  "required": [
    "object_id",
    "extended_info"
  ],
  "type": "object"
}
```

</details>
