# riverhog-ftp-adapter:configuration:source-config configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:riverhog-ftp-adapter:riverhog-ftp-adapter-configuration-source-42c0c01de3:38ed5e4a08 -->

One deployment-owned, content-opaque intake source.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Configuration Documents](index.md) |
| Contract elements | 1 |
| Extent decisions | 8 |

## External contract

<a id="s-6faee706ac"></a>
- <a id="s-ad8c226b83"></a>`title`: SourceConfig
- <a id="s-0e345f7a8e"></a>`description`: One deployment-owned, content-opaque intake source.
- <a id="s-c6d70e7ca8"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-44782b2557"></a>`archive_store` | no | anyOf=type="string"; minLength=1; maxLength=160 \| type="null" |  |
| <a id="s-d8f63ea908"></a>`close_mode` | no | type="string"; enum=["stable","explicit-flush"] |  |
| <a id="s-05492fae9c"></a>`description` | no | anyOf=#/$defs/CollectionDescription \| type="null" |  |
| <a id="s-ae0af366ef"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |
| <a id="s-7147ff2557"></a>`ingest_source` | yes | type="string"; minLength=1; maxLength=512 |  |
| <a id="s-5e54358858"></a>`max_bytes` | no | type="integer"; minimum=1 |  |
| <a id="s-f3e227fccb"></a>`max_files` | no | type="integer"; minimum=1 |  |
| <a id="s-cf8393c1e2"></a>`provenance` | no | type="string"; enum=["capture","omit"] |  |
| <a id="s-5be0fa673e"></a>`provenance_omission_reason` | no | anyOf=type="string"; maxLength=1000 \| type="null" |  |
| <a id="s-4a38ed53f2"></a>`root` | yes | type="string"; format="path" |  |
| <a id="s-eea19079fd"></a>`tags` | no | type="array"; items=(#/$defs/CollectionTag) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-fb60852aae"></a>`CollectionDescription` | type="string"; minLength=1; maxLength=32768; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-de506e6f37"></a>`CollectionTag` | type="string"; minLength=1; maxLength=65536; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-ftp-adapter:configuration:source-config"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field tags](#s-eea19079fd) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition CollectionDescription](#s-fb60852aae) | `encoded-size · bytes · contract_max` | maximum=32768; reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionDescription](#s-fb60852aae) | `length · characters · contract_max` | maximum=32768; minimum=1; reason="schema-maximum" |
| [definition CollectionTag](#s-de506e6f37) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionTag](#s-de506e6f37) | `length · characters · contract_max` | maximum=65536; minimum=1; reason="schema-maximum" |
| <a id="s-628c261c8f"></a>[field archive_store · string value](#s-44782b2557) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field ingest_source](#s-7147ff2557) | `length · characters · contract_max` | maximum=512; minimum=1; reason="schema-maximum" |
| <a id="s-cb3b2c44f1"></a>[field provenance_omission_reason · string value](#s-5be0fa673e) | `length · characters · contract_max` | maximum=1000; reason="schema-maximum" |

## Governing policies

- <a id="pa-8b125b3006"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-ee9d96eb28"></a>[extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)
- <a id="pa-70e341ff46"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration:riverhog-ftp-adapter:configuration:source-config](../../../evidence/sources.md#src-6674ffa9af) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py::SourceConfig`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/riverhog-ftp-adapter:configuration:source-config`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 814e86c57a33d78f0992459b58d920b897b9707c9376143172c9d60d967cd496 -->

```json
{
  "$defs": {
    "CollectionDescription": {
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
    "CollectionTag": {
      "maxLength": 65536,
      "minLength": 1,
      "type": "string",
      "x-riverhog-encoded-bytes-max": 65536,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-human-authored-collection-tag"
      },
      "x-unicode-normalization": "NFC"
    }
  },
  "additionalProperties": false,
  "description": "One deployment-owned, content-opaque intake source.",
  "properties": {
    "archive_store": {
      "anyOf": [
        {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Archive Store"
    },
    "close_mode": {
      "default": "stable",
      "enum": [
        "stable",
        "explicit-flush"
      ],
      "title": "Close Mode",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "$ref": "#/$defs/CollectionDescription"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "ingest_source": {
      "maxLength": 512,
      "minLength": 1,
      "title": "Ingest Source",
      "type": "string"
    },
    "max_bytes": {
      "default": 107374182400,
      "minimum": 1,
      "title": "Max Bytes",
      "type": "integer"
    },
    "max_files": {
      "default": 1000,
      "minimum": 1,
      "title": "Max Files",
      "type": "integer"
    },
    "provenance": {
      "default": "capture",
      "enum": [
        "capture",
        "omit"
      ],
      "title": "Provenance",
      "type": "string"
    },
    "provenance_omission_reason": {
      "anyOf": [
        {
          "maxLength": 1000,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Provenance Omission Reason"
    },
    "root": {
      "format": "path",
      "title": "Root",
      "type": "string"
    },
    "tags": {
      "default": [],
      "items": {
        "$ref": "#/$defs/CollectionTag"
      },
      "title": "Tags",
      "type": "array"
    }
  },
  "required": [
    "id",
    "root",
    "ingest_source"
  ],
  "title": "SourceConfig",
  "type": "object"
}
```
