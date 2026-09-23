# a-riverhog-ftp-spool:configuration:source-config configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:a-riverhog-ftp-spool:a-riverhog-ftp-spool-configuration-source-f67b65873b:91154b0274 -->

One deployment-owned, content-opaque intake source.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-7431ad5a8c"></a>

- <a id="s-37e50ca865"></a>`type`: `"object"`
- <a id="s-9c1537c843"></a>`additionalProperties`: `false`
- <a id="s-77ef4173f5"></a>`description`: `"One deployment-owned, content-opaque intake source."`
- <a id="s-57070b1458"></a>`required`: `["id","root","ingest_source"]`
- <a id="s-9401567fe3"></a>`title`: `"SourceConfig"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-54988fd422"></a>`archive_store` | no | anyOf=[(type="string"; maxLength=160; minLength=1); (type="null")]; default=null; title="Archive Store" |  |
| <a id="s-fd7f658f70"></a>`close_mode` | no | type="string"; enum=["stable","explicit-flush"]; default="stable"; title="Close Mode" |  |
| <a id="s-0ba36845ee"></a>`description` | no | anyOf=[([CollectionDescription](#s-7e6c3aae92)); (type="null")]; default=null |  |
| <a id="s-0d847fa7fe"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Id" |  |
| <a id="s-a1e84d0162"></a>`ingest_source` | yes | type="string"; maxLength=512; minLength=1; title="Ingest Source" |  |
| <a id="s-f8bc079e6c"></a>`max_bytes` | no | type="integer"; minimum=1; default=107374182400; title="Max Bytes" |  |
| <a id="s-09627455ff"></a>`max_files` | no | type="integer"; minimum=1; default=1000; title="Max Files" |  |
| <a id="s-17ce280b63"></a>`provenance` | no | type="string"; enum=["capture","omit"]; default="capture"; title="Provenance" |  |
| <a id="s-ceaa7ea925"></a>`provenance_omission_reason` | no | anyOf=[(type="string"; maxLength=1000); (type="null")]; default=null; title="Provenance Omission Reason" |  |
| <a id="s-820a0c89f4"></a>`root` | yes | type="string"; format="path"; title="Root" |  |
| <a id="s-f0388cf32c"></a>`tags` | no | type="array"; default=[]; items=([CollectionTag](#s-531ca7b499)); title="Tags" |  |

### Definitions

- [CollectionDescription](#s-7e6c3aae92)
- [CollectionTag](#s-531ca7b499)

### <a id="s-7e6c3aae92"></a>definition `CollectionDescription`

- <a id="s-bbe655eab1"></a>`type`: `"string"`
- <a id="s-ce768f8521"></a>`maxLength`: `32768`
- <a id="s-001d445678"></a>`minLength`: `1`
- <a id="s-234ce21c4f"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-e7a14adf45"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-14a9243b8f"></a>`x-unicode-normalization`: `"NFC"`

### <a id="s-531ca7b499"></a>definition `CollectionTag`

- <a id="s-bc1178c651"></a>`type`: `"string"`
- <a id="s-95ea6978d7"></a>`maxLength`: `65536`
- <a id="s-e362e71824"></a>`minLength`: `1`
- <a id="s-db5b5cc6fe"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-9a1ac96840"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-698d6bfaee"></a>`x-unicode-normalization`: `"NFC"`

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-ftp-spool:configuration:source-config"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field tags](#s-f0388cf32c) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition CollectionDescription](#s-7e6c3aae92) | `encoded-size · bytes · contract_max` | maximum=32768; reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionDescription](#s-7e6c3aae92) | `length · characters · contract_max` | maximum=32768; minimum=1; reason="schema-maximum" |
| [definition CollectionTag](#s-531ca7b499) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionTag](#s-531ca7b499) | `length · characters · contract_max` | maximum=65536; minimum=1; reason="schema-maximum" |
| <a id="s-f63d010858"></a>[field archive_store · string value](#s-54988fd422) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field ingest_source](#s-a1e84d0162) | `length · characters · contract_max` | maximum=512; minimum=1; reason="schema-maximum" |
| <a id="s-bf37adf15f"></a>[field provenance_omission_reason · string value](#s-ceaa7ea925) | `length · characters · contract_max` | maximum=1000; reason="schema-maximum" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-601d6c5dcf"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-dbaa47bfdd"></a>[extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)
- <a id="pa-ed4e2be1c8"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:a-riverhog-ftp-spool:configuration:source-config](../../../evidence/sources/authorities.md#src-9f7f08e715) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/config.py::SourceConfig](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/a-riverhog-ftp-spool:configuration:source-config`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
