# Riverhog v1 recovery descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-archive-contracts:riverhog-v1-recovery-descriptor:29e07b6fb5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-66d562e63c) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-d0e22522fc"></a>
- <a id="s-73491a8562"></a>`$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json
- <a id="s-9788b6bdfd"></a>`title`: Riverhog v1 recovery descriptor
- <a id="s-a1f5264f4a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d907c3144a"></a>`encryption` | yes | type="object"; fields=`format`, `passphrase_id`; additional keys=`additionalProperties`, `required` |  |
| <a id="s-2b019864cd"></a>`root` | yes | type="object"; fields=`path`, `stored_bytes`, `stored_sha256`; additional keys=`additionalProperties`, `required` |  |
| <a id="s-d0b998e46c"></a>`schema` | yes | const="riverhog-recovery-descriptor/v1" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-abd13e34a1"></a>[field root · field stored_bytes](#s-2b019864cd) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b3eb015b6d"></a>[field root · field stored_sha256](#s-2b019864cd) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-0d8237df70"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-840c430459"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-bce6ec5613"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json](../../../evidence/sources.md#src-bd3602393c) — `packages/riverhog-archive-contracts/schemas/riverhog-recovery-descriptor-v1.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-recovery-descriptor-v1.schema.json`

### Exact owned JSON

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
