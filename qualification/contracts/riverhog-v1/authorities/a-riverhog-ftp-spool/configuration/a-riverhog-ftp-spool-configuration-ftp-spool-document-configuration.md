# a-riverhog-ftp-spool:configuration:ftp-spool-document configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:a-riverhog-ftp-spool:a-riverhog-ftp-spool-configuration-ftp-sp-47b89bae27:47b1fa4054 -->

Operator document; credentials remain file paths until policy is valid.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-bb22ac3d7b"></a>

- <a id="s-4dd1b15df5"></a>`type`: `"object"`
- <a id="s-286948c836"></a>`additionalProperties`: `false`
- <a id="s-6bea6a9214"></a>`description`: `"Operator document; credentials remain file paths until policy is valid."`
- <a id="s-3804f0c70e"></a>`required`: `["host_id","riverhog_base_url","sources","riverhog_token_file","api_token_file"]`
- <a id="s-f1fe3ab89b"></a>`title`: `"FtpSpoolDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ba03f2ef3a"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-5bba2ffb09"></a>`api_token_file` | yes | type="string"; format="path"; title="Api Token File" |  |
| <a id="s-4d71139572"></a>`claim_attempt_budget` | no | type="integer"; minimum=2; default=8; title="Claim Attempt Budget" |  |
| <a id="s-db2970ae39"></a>`completion_failure_attempt_budget` | no | type="integer"; minimum=1; default=8; title="Completion Failure Attempt Budget" |  |
| <a id="s-ad57bb4d14"></a>`completion_failure_capacity` | no | type="integer"; minimum=1; default=128; title="Completion Failure Capacity" |  |
| <a id="s-b66839df02"></a>`discovery_entry_budget` | no | type="integer"; minimum=1; default=4096; title="Discovery Entry Budget" |  |
| <a id="s-83ac020fa0"></a>`host_id` | yes | type="string"; maxLength=255; minLength=1; title="Host Id" |  |
| <a id="s-ef771008a4"></a>`pending_claim_capacity` | no | type="integer"; minimum=1; default=128; title="Pending Claim Capacity" |  |
| <a id="s-37315c1895"></a>`poll_seconds` | no | type="number"; minimum=0.1; maximum=3600; default=5; title="Poll Seconds" |  |
| <a id="s-f0cabc4a69"></a>`provenance_observer` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Provenance Observer" |  |
| <a id="s-f8a1f734c6"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1; title="Riverhog Base Url" |  |
| <a id="s-e8c2da7f6a"></a>`riverhog_token_file` | yes | type="string"; format="path"; title="Riverhog Token File" |  |
| <a id="s-84c0e6e010"></a>`sources` | yes | type="array"; items=([SourceConfig](#s-060968562c)); minItems=1; title="Sources" |  |

### Definitions

- [CollectionDescription](#s-e497105c64)
- [CollectionTag](#s-93946831b2)
- [SourceConfig](#s-060968562c)

### <a id="s-e497105c64"></a>definition `CollectionDescription`

- <a id="s-476736d04a"></a>`type`: `"string"`
- <a id="s-802a4be1ee"></a>`maxLength`: `32768`
- <a id="s-1b0f3795ef"></a>`minLength`: `1`
- <a id="s-02085132bb"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-bd02c147c1"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-be69466de9"></a>`x-unicode-normalization`: `"NFC"`

### <a id="s-93946831b2"></a>definition `CollectionTag`

- <a id="s-5a0ab0687b"></a>`type`: `"string"`
- <a id="s-5ac84b0ae9"></a>`maxLength`: `65536`
- <a id="s-ff851c8451"></a>`minLength`: `1`
- <a id="s-29e729d6e5"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-25919fd3b2"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-eff61bdd3e"></a>`x-unicode-normalization`: `"NFC"`

### <a id="s-060968562c"></a>definition `SourceConfig`

- <a id="s-8b0504ab8f"></a>`type`: `"object"`
- <a id="s-96e99687dd"></a>`additionalProperties`: `false`
- <a id="s-b15b8bce7e"></a>`description`: `"One deployment-owned, content-opaque intake source."`
- <a id="s-7679ac3a3b"></a>`required`: `["id","root","ingest_source"]`
- <a id="s-3c000cc39a"></a>`title`: `"SourceConfig"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-59de691d56"></a>`archive_store` | no | anyOf=[(type="string"; maxLength=160; minLength=1); (type="null")]; default=null; title="Archive Store" |  |
| <a id="s-e0627e7b65"></a>`close_mode` | no | type="string"; enum=["stable","explicit-flush"]; default="stable"; title="Close Mode" |  |
| <a id="s-f0e385ea96"></a>`description` | no | anyOf=[([CollectionDescription](#s-e497105c64)); (type="null")]; default=null |  |
| <a id="s-11d399c7ac"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Id" |  |
| <a id="s-d69e1b8ad2"></a>`ingest_source` | yes | type="string"; maxLength=512; minLength=1; title="Ingest Source" |  |
| <a id="s-1d774d6def"></a>`max_bytes` | no | type="integer"; minimum=1; default=107374182400; title="Max Bytes" |  |
| <a id="s-035f2b2276"></a>`max_files` | no | type="integer"; minimum=1; default=1000; title="Max Files" |  |
| <a id="s-d9aec81427"></a>`provenance` | no | type="string"; enum=["capture","omit"]; default="capture"; title="Provenance" |  |
| <a id="s-80f98a74b6"></a>`provenance_omission_reason` | no | anyOf=[(type="string"; maxLength=1000); (type="null")]; default=null; title="Provenance Omission Reason" |  |
| <a id="s-78e9570ab0"></a>`root` | yes | type="string"; format="path"; title="Root" |  |
| <a id="s-e93151fac9"></a>`tags` | no | type="array"; default=[]; items=([CollectionTag](#s-93946831b2)); title="Tags" |  |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-ftp-spool:configuration:ftp-spool-document"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition SourceConfig · field tags](#s-e93151fac9) | `cardinality · items · operational_policy` | shared above |
| [field sources](#s-84c0e6e010) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition CollectionDescription](#s-e497105c64) | `encoded-size · bytes · contract_max` | maximum=32768; reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionDescription](#s-e497105c64) | `length · characters · contract_max` | maximum=32768; minimum=1; reason="schema-maximum" |
| [definition CollectionTag](#s-93946831b2) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionTag](#s-93946831b2) | `length · characters · contract_max` | maximum=65536; minimum=1; reason="schema-maximum" |
| <a id="s-f49f543914"></a>[definition SourceConfig · field archive_store · string value](#s-59de691d56) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [definition SourceConfig · field ingest_source](#s-d69e1b8ad2) | `length · characters · contract_max` | maximum=512; minimum=1; reason="schema-maximum" |
| <a id="s-1c6de4b4ec"></a>[definition SourceConfig · field provenance_omission_reason · string value](#s-80f98a74b6) | `length · characters · contract_max` | maximum=1000; reason="schema-maximum" |
| [field host_id](#s-83ac020fa0) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field poll_seconds](#s-37315c1895) | `value · schema-value · contract_max` | maximum=3600; minimum=0.1; reason="schema-maximum" |
| <a id="s-9b0aba7c58"></a>[field provenance_observer · string value](#s-f0cabc4a69) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field riverhog_base_url](#s-f8a1f734c6) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-07d26e11ba"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-5a3da44fda"></a>[extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)
- <a id="pa-d382605210"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:a-riverhog-ftp-spool:configuration:ftp-spool-document](../../../evidence/sources/authorities.md#src-ace1740d68) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/config.py::FtpSpoolDocument](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/a-riverhog-ftp-spool:configuration:ftp-spool-document`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 795f974e76d2b304ef68bb5f3153b80076b5fc1f55cffef4252177b92bfccd03 -->

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
    },
    "SourceConfig": {
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
  },
  "additionalProperties": false,
  "description": "Operator document; credentials remain file paths until policy is valid.",
  "properties": {
    "allow_insecure_http": {
      "default": false,
      "title": "Allow Insecure Http",
      "type": "boolean"
    },
    "api_token_file": {
      "format": "path",
      "title": "Api Token File",
      "type": "string"
    },
    "claim_attempt_budget": {
      "default": 8,
      "minimum": 2,
      "title": "Claim Attempt Budget",
      "type": "integer"
    },
    "completion_failure_attempt_budget": {
      "default": 8,
      "minimum": 1,
      "title": "Completion Failure Attempt Budget",
      "type": "integer"
    },
    "completion_failure_capacity": {
      "default": 128,
      "minimum": 1,
      "title": "Completion Failure Capacity",
      "type": "integer"
    },
    "discovery_entry_budget": {
      "default": 4096,
      "minimum": 1,
      "title": "Discovery Entry Budget",
      "type": "integer"
    },
    "host_id": {
      "maxLength": 255,
      "minLength": 1,
      "title": "Host Id",
      "type": "string"
    },
    "pending_claim_capacity": {
      "default": 128,
      "minimum": 1,
      "title": "Pending Claim Capacity",
      "type": "integer"
    },
    "poll_seconds": {
      "default": 5,
      "maximum": 3600,
      "minimum": 0.1,
      "title": "Poll Seconds",
      "type": "number"
    },
    "provenance_observer": {
      "anyOf": [
        {
          "maxLength": 255,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Provenance Observer"
    },
    "riverhog_base_url": {
      "maxLength": 2048,
      "minLength": 1,
      "title": "Riverhog Base Url",
      "type": "string"
    },
    "riverhog_token_file": {
      "format": "path",
      "title": "Riverhog Token File",
      "type": "string"
    },
    "sources": {
      "items": {
        "$ref": "#/$defs/SourceConfig"
      },
      "minItems": 1,
      "title": "Sources",
      "type": "array"
    }
  },
  "required": [
    "host_id",
    "riverhog_base_url",
    "sources",
    "riverhog_token_file",
    "api_token_file"
  ],
  "title": "FtpSpoolDocument",
  "type": "object"
}
```

</details>
