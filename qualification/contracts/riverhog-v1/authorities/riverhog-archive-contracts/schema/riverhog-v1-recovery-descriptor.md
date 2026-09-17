# Riverhog v1 recovery descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-archive-contracts:riverhog-v1-recovery-descriptor:ba4d505914 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-d0e22522fc"></a>

- <a id="s-a1f5264f4a"></a>`type`: `"object"`
- <a id="s-73491a8562"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json"`
- <a id="s-b6e231d4d9"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-1a229fead9"></a>`additionalProperties`: `false`
- <a id="s-59821c0437"></a>`required`: `["schema","encryption","root"]`
- <a id="s-9788b6bdfd"></a>`title`: `"Riverhog v1 recovery descriptor"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `encryption` | yes | [See field `encryption`](#s-d907c3144a) |  |
| `root` | yes | [See field `root`](#s-2b019864cd) |  |
| <a id="s-d0b998e46c"></a>`schema` | yes | const="riverhog-recovery-descriptor/v1" |  |

### <a id="s-d907c3144a"></a>field `encryption`

- <a id="s-0adbe1a054"></a>`type`: `"object"`
- <a id="s-ecb5c38996"></a>`additionalProperties`: `false`
- <a id="s-612a673251"></a>`required`: `["format","passphrase_id"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b70ff1741a"></a>`format` | yes | const="age-v1-scrypt" |  |
| <a id="s-9f0bf45f71"></a>`passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$" |  |

### <a id="s-2b019864cd"></a>field `root`

- <a id="s-7be2244482"></a>`type`: `"object"`
- <a id="s-aa195ac116"></a>`additionalProperties`: `false`
- <a id="s-3663c6f878"></a>`required`: `["path","stored_bytes","stored_sha256"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-46a6ba0c8e"></a>`path` | yes | const="manifest.json.age" |  |
| <a id="s-abd13e34a1"></a>`stored_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-b3eb015b6d"></a>`stored_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field root · field stored_bytes](#s-abd13e34a1) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field root · field stored_sha256](#s-b3eb015b6d) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-a0f0183050"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-7dbe077ea6"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-20497f1ec0"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json](../../../evidence/sources/authorities.md#src-bd3602393c) — [packages/riverhog-archive-contracts/schemas/riverhog-recovery-descriptor-v1.schema.json](../../../../../../packages/riverhog-archive-contracts/schemas/riverhog-recovery-descriptor-v1.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-recovery-descriptor-v1.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 70e187d12431366bc3f4ef919e9afca355e3ec4ca9ba8a7cb1a98a393ed034fb -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "encryption": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "age-v1-scrypt"
        },
        "passphrase_id": {
          "pattern": "^[A-Za-z0-9_-]{16,128}$",
          "type": "string"
        }
      },
      "required": [
        "format",
        "passphrase_id"
      ],
      "type": "object"
    },
    "root": {
      "additionalProperties": false,
      "properties": {
        "path": {
          "const": "manifest.json.age"
        },
        "stored_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "stored_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "path",
        "stored_bytes",
        "stored_sha256"
      ],
      "type": "object"
    },
    "schema": {
      "const": "riverhog-recovery-descriptor/v1"
    }
  },
  "required": [
    "schema",
    "encryption",
    "root"
  ],
  "title": "Riverhog v1 recovery descriptor",
  "type": "object"
}
```

</details>
