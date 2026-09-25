# FtpSpoolConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:a-riverhog-ftp-spool:ftpspoolconfig:7e41ae7fd9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-801f6f03b3"></a>

- <a id="s-11951486fa"></a>`type`: `"object"`
- <a id="s-317e82bb8b"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/a-riverhog-ftp-spool.schema.json"`
- <a id="s-25274d4d78"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-7b852aae98"></a>`additionalProperties`: `false`
- <a id="s-ed297c6c47"></a>`required`: `["host_id","riverhog_base_url","riverhog_token_file","api_token_file","sources"]`
- <a id="s-0e81af0eea"></a>`title`: `"FtpSpoolConfig"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f559e56b92"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-f7b70db9af"></a>`api_token_file` | yes | type="string"; minLength=1 |  |
| <a id="s-27a6dab01a"></a>`claim_attempt_budget` | no | type="integer"; minimum=2; default=8; title="Claim Attempt Budget" |  |
| <a id="s-96027444fd"></a>`completion_failure_attempt_budget` | no | type="integer"; minimum=1; default=8; title="Completion Failure Attempt Budget" |  |
| <a id="s-60760bd4a7"></a>`completion_failure_capacity` | no | type="integer"; minimum=1; default=128; title="Completion Failure Capacity" |  |
| <a id="s-6f93d1e949"></a>`discovery_entry_budget` | no | type="integer"; minimum=1; default=4096; title="Discovery Entry Budget" |  |
| <a id="s-679c9ff813"></a>`host_id` | yes | type="string"; maxLength=255; minLength=1; title="Host Id" |  |
| <a id="s-350ee6a591"></a>`pending_claim_capacity` | no | type="integer"; minimum=1; default=128; title="Pending Claim Capacity" |  |
| <a id="s-e82ec3bd4e"></a>`poll_seconds` | no | type="number"; minimum=0.1; maximum=3600; default=5; title="Poll Seconds" |  |
| <a id="s-5f0ce0ece1"></a>`provenance_observer` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Provenance Observer" |  |
| <a id="s-0285779807"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1; title="Riverhog Base Url" |  |
| <a id="s-a00a4344b9"></a>`riverhog_token_file` | yes | type="string"; minLength=1 |  |
| <a id="s-d94e125022"></a>`sources` | yes | type="array"; items=([SourceConfig](#s-0f1ce11bd4)); minItems=1; title="Sources" |  |

### Definitions

- [CollectionDescription](#s-2d44f53dd1)
- [CollectionTag](#s-68095e5108)
- [SourceConfig](#s-0f1ce11bd4)

### <a id="s-2d44f53dd1"></a>definition `CollectionDescription`

- <a id="s-d75ad2a00d"></a>`type`: `"string"`
- <a id="s-dd546850fa"></a>`maxLength`: `32768`
- <a id="s-00cd7c38b4"></a>`minLength`: `1`
- <a id="s-39bf0b238c"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-93b91393df"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-8b547eff93"></a>`x-unicode-normalization`: `"NFC"`

### <a id="s-68095e5108"></a>definition `CollectionTag`

- <a id="s-55706d011d"></a>`type`: `"string"`
- <a id="s-1e2479c9de"></a>`maxLength`: `65536`
- <a id="s-1e5d444bcf"></a>`minLength`: `1`
- <a id="s-53541b56af"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-6077d8ab8b"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-c87dd33a91"></a>`x-unicode-normalization`: `"NFC"`

### <a id="s-0f1ce11bd4"></a>definition `SourceConfig`

- <a id="s-5e072385f6"></a>`type`: `"object"`
- <a id="s-8c7c2f3783"></a>`additionalProperties`: `false`
- <a id="s-fd07396614"></a>`description`: `"One deployment-owned, content-opaque intake source."`
- <a id="s-64f098e33f"></a>`required`: `["id","root","ingest_source"]`
- <a id="s-67c7bd0335"></a>`title`: `"SourceConfig"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b96a7c9ff0"></a>`archive_store` | no | anyOf=[(type="string"; maxLength=160; minLength=1); (type="null")]; default=null; title="Archive Store" |  |
| <a id="s-fc8ede723a"></a>`close_mode` | no | type="string"; enum=["stable","explicit-flush"]; default="stable"; title="Close Mode" |  |
| <a id="s-44b178bf1d"></a>`description` | no | anyOf=[([CollectionDescription](#s-2d44f53dd1)); (type="null")]; default=null |  |
| <a id="s-488e8277ab"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Id" |  |
| <a id="s-a93bc5febd"></a>`ingest_source` | yes | type="string"; maxLength=512; minLength=1; title="Ingest Source" |  |
| <a id="s-eca2a25abe"></a>`max_bytes` | no | type="integer"; minimum=1; default=107374182400; title="Max Bytes" |  |
| <a id="s-8a0e3944a7"></a>`max_files` | no | type="integer"; minimum=1; default=1000; title="Max Files" |  |
| <a id="s-0ef8302f5c"></a>`provenance` | no | type="string"; enum=["capture","omit"]; default="capture"; title="Provenance" |  |
| <a id="s-bc41e02b28"></a>`provenance_omission_reason` | no | anyOf=[(type="string"; maxLength=1000); (type="null")]; default=null; title="Provenance Omission Reason" |  |
| <a id="s-e5ea9df001"></a>`root` | yes | type="string"; format="path"; title="Root" |  |
| <a id="s-7a1dc89b9d"></a>`tags` | no | type="array"; default=[]; items=([CollectionTag](#s-68095e5108)); title="Tags" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/config/a-riverhog-ftp-spool.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition SourceConfig · field tags](#s-7a1dc89b9d) | `cardinality · items · operational_policy` | shared above |
| [field sources](#s-d94e125022) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition CollectionDescription](#s-2d44f53dd1) | `encoded-size · bytes · contract_max` | maximum=32768; reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionDescription](#s-2d44f53dd1) | `length · characters · contract_max` | maximum=32768; minimum=1; reason="schema-maximum" |
| [definition CollectionTag](#s-68095e5108) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionTag](#s-68095e5108) | `length · characters · contract_max` | maximum=65536; minimum=1; reason="schema-maximum" |
| <a id="s-f6e72a3c2b"></a>[definition SourceConfig · field archive_store · string value](#s-b96a7c9ff0) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [definition SourceConfig · field ingest_source](#s-a93bc5febd) | `length · characters · contract_max` | maximum=512; minimum=1; reason="schema-maximum" |
| <a id="s-a1abc91fd9"></a>[definition SourceConfig · field provenance_omission_reason · string value](#s-bc41e02b28) | `length · characters · contract_max` | maximum=1000; reason="schema-maximum" |
| [field host_id](#s-679c9ff813) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field poll_seconds](#s-e82ec3bd4e) | `value · schema-value · contract_max` | maximum=3600; minimum=0.1; reason="schema-maximum" |
| <a id="s-1a130efa87"></a>[field provenance_observer · string value](#s-5f0ce0ece1) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field riverhog_base_url](#s-0285779807) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-98787743dc"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-c394699d77"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-3cbba6915a"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/config/a-riverhog-ftp-spool.schema.json](../../../evidence/sources/authorities.md#src-5983d55f00) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/config.schema.json](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/config.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1config~1a-riverhog-ftp-spool.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 72f5ae78f5aab2ef8df19532d083a62e0e2f63bf525201ba07471bae247baf89 -->

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
  "$id": "https://nashspence.github.io/riverhog/v1/config/a-riverhog-ftp-spool.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "allow_insecure_http": {
      "default": false,
      "title": "Allow Insecure Http",
      "type": "boolean"
    },
    "api_token_file": {
      "minLength": 1,
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
      "minLength": 1,
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
    "riverhog_token_file",
    "api_token_file",
    "sources"
  ],
  "title": "FtpSpoolConfig",
  "type": "object"
}
```

</details>
