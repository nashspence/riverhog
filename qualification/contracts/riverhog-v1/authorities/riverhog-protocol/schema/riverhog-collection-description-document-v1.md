# Riverhog collection description document v1

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-protocol:riverhog-collection-description-document-v1:3e1f6c28c0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-8609cbea6f"></a>

- <a id="s-ca6e9ba2c0"></a>`type`: `"object"`
- <a id="s-9b49978133"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json"`
- <a id="s-53e4404928"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-9b6566a588"></a>`additionalProperties`: `false`
- <a id="s-56351d245d"></a>`required`: `["archive_root_sha256","description","description_identity","format","revision"]`
- <a id="s-b6e2c77d61"></a>`title`: `"Riverhog collection description document v1"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-884eb7f63f"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-908415d773"></a>`description` | yes | oneOf=[(type="string"; maxLength=32768; minLength=1; x-riverhog-encoded-bytes-max=32768; x-riverhog-extent={"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}; x-unicode-normalization="NFC"); (type="null")] |  |
| <a id="s-2ab1f766a7"></a>`description_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-49e4a4e153"></a>`format` | yes | const="riverhog-collection-description/v1" |  |
| <a id="s-db04814e8f"></a>`revision` | yes | type="integer"; minimum=1; maximum=9007199254740991; x-riverhog-extent={"policy":"fixed","reason":"exact-json-safe-monotonic-description-revision"} |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_root_sha256](#s-884eb7f63f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-f0c992f586"></a>[field description · string value](#s-908415d773) | `encoded-size · bytes · contract_max` | maximum=32768; reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field description · string value](#s-f0c992f586) | `length · characters · contract_max` | maximum=32768; minimum=1; reason="schema-maximum" |
| [field description_identity](#s-2ab1f766a7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field revision](#s-db04814e8f) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-e7fd130974"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-20092265c7"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json](../../../evidence/sources/authorities.md#src-57bde0d90d) — [packages/riverhog-protocol/schemas/riverhog-collection-description-v1.schema.json](../../../../../../packages/riverhog-protocol/schemas/riverhog-collection-description-v1.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-collection-description-v1.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
