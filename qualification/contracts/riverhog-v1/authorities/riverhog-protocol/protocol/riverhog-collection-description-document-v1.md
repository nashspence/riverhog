# Riverhog collection description document v1

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-protocol:riverhog-collection-description-document-v1:6266f2d39a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-7249fc431908) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-8609cbea6f2d"></a>
- <a id="s-9b49978133ba"></a>`$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json
- <a id="s-b6e2c77d6139"></a>`title`: Riverhog collection description document v1
- <a id="s-ca6e9ba2c059"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-884eb7f63f48"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-908415d773d9"></a>`description` | yes | oneOf=type="string"; minLength=1; maxLength=32768; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` \| type="null" |  |
| <a id="s-2ab1f766a7c8"></a>`description_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-49e4a4e15315"></a>`format` | yes | const="riverhog-collection-description/v1" |  |
| <a id="s-db04814e8ff1"></a>`revision` | yes | type="integer"; minimum=1; maximum=9007199254740991; additional keys=`x-riverhog-extent` |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_root_sha256](#s-884eb7f63f48) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-f0c992f586c6"></a>field description · oneOf alternative 1 | `encoded-size · bytes · contract_max` | maximum=32768; reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field description · oneOf alternative 1](#s-f0c992f586c6) | `length · characters · contract_max` | maximum=32768; minimum=1; reason="schema-maximum" |
| [field description_identity](#s-2ab1f766a7c8) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field revision](#s-db04814e8ff1) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-86d1ed428bf4"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-1539664b0c81"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json](../../../evidence/sources.md#src-57bde0d90d0d) — `packages/riverhog-protocol/schemas/riverhog-collection-description-v1.schema.json`

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
