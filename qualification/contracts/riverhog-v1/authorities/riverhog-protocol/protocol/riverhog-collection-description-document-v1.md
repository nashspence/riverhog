# Riverhog collection description document v1

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-protocol:riverhog-collection-description-document-v1:6266f2d39a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-7249fc4319) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-8609cbea6f"></a>
- <a id="s-9b49978133"></a>`$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json
- <a id="s-b6e2c77d61"></a>`title`: Riverhog collection description document v1
- <a id="s-ca6e9ba2c0"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-884eb7f63f"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-908415d773"></a>`description` | yes | oneOf=type="string"; minLength=1; maxLength=32768; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` \| type="null" |  |
| <a id="s-2ab1f766a7"></a>`description_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-49e4a4e153"></a>`format` | yes | const="riverhog-collection-description/v1" |  |
| <a id="s-db04814e8f"></a>`revision` | yes | type="integer"; minimum=1; maximum=9007199254740991; additional keys=`x-riverhog-extent` |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_root_sha256](#s-884eb7f63f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-f0c992f586"></a>[field description · string value](#s-908415d773) | `encoded-size · bytes · contract_max` | maximum=32768; reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field description · string value](#s-f0c992f586) | `length · characters · contract_max` | maximum=32768; minimum=1; reason="schema-maximum" |
| [field description_identity](#s-2ab1f766a7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field revision](#s-db04814e8f) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-86d1ed428b"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-1539664b0c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json](../../../evidence/sources.md#src-57bde0d90d) — `packages/riverhog-protocol/schemas/riverhog-collection-description-v1.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-collection-description-v1.schema.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc0c384f58432075d0c9df58afd0af1c50307749fa4873380bb8bb093c2ad9c8 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "archive_root_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    },
    "description": {
      "oneOf": [
        {
          "maxLength": 32768,
          "minLength": 1,
          "type": "string",
          "x-riverhog-encoded-bytes-max": 32768,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-human-authored-catalog-description"
          },
          "x-unicode-normalization": "NFC"
        },
        {
          "type": "null"
        }
      ]
    },
    "description_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    },
    "format": {
      "const": "riverhog-collection-description/v1"
    },
    "revision": {
      "maximum": 9007199254740991,
      "minimum": 1,
      "type": "integer",
      "x-riverhog-extent": {
        "policy": "fixed",
        "reason": "exact-json-safe-monotonic-description-revision"
      }
    }
  },
  "required": [
    "archive_root_sha256",
    "description",
    "description_identity",
    "format",
    "revision"
  ],
  "title": "Riverhog collection description document v1",
  "type": "object"
}
```
