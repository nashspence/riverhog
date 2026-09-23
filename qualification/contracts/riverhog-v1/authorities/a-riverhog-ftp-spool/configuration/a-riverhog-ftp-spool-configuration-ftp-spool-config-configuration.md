# a-riverhog-ftp-spool:configuration:ftp-spool-config configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:a-riverhog-ftp-spool:a-riverhog-ftp-spool-configuration-ftp-sp-b39b3ce3ac:13753e4816 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-a13c122188"></a>

- <a id="s-3da27421c6"></a>`type`: `"object"`
- <a id="s-ee0e3cbfa6"></a>`additionalProperties`: `false`
- <a id="s-d7487033b9"></a>`required`: `["host_id","riverhog_base_url","riverhog_token","api_token","sources"]`
- <a id="s-7c3112d795"></a>`title`: `"FtpSpoolConfig"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7bf6b7de4b"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-15550dd9ff"></a>`api_token` | yes | type="string"; maxLength=4096; minLength=1; title="Api Token" |  |
| <a id="s-1991aed3be"></a>`claim_attempt_budget` | no | type="integer"; minimum=2; default=8; title="Claim Attempt Budget" |  |
| <a id="s-0a2027c938"></a>`completion_failure_attempt_budget` | no | type="integer"; minimum=1; default=8; title="Completion Failure Attempt Budget" |  |
| <a id="s-3272156e39"></a>`completion_failure_capacity` | no | type="integer"; minimum=1; default=128; title="Completion Failure Capacity" |  |
| <a id="s-e28d2cffcf"></a>`discovery_entry_budget` | no | type="integer"; minimum=1; default=4096; title="Discovery Entry Budget" |  |
| <a id="s-bad31974cd"></a>`host_id` | yes | type="string"; maxLength=255; minLength=1; title="Host Id" |  |
| <a id="s-6a8087a468"></a>`pending_claim_capacity` | no | type="integer"; minimum=1; default=128; title="Pending Claim Capacity" |  |
| <a id="s-b30dc7fc71"></a>`poll_seconds` | no | type="number"; minimum=0.1; maximum=3600; default=5; title="Poll Seconds" |  |
| <a id="s-06296f46ed"></a>`provenance_observer` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Provenance Observer" |  |
| <a id="s-9c7f62aa90"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1; title="Riverhog Base Url" |  |
| <a id="s-b5fcc83070"></a>`riverhog_token` | yes | type="string"; maxLength=4096; minLength=1; title="Riverhog Token" |  |
| <a id="s-b1bab85334"></a>`sources` | yes | type="array"; items=([SourceConfig](#s-2ff1393c2b)); minItems=1; title="Sources" |  |

### Definitions

- [CollectionDescription](#s-a6bdda00e8)
- [CollectionTag](#s-436bd9bf7b)
- [SourceConfig](#s-2ff1393c2b)

### <a id="s-a6bdda00e8"></a>definition `CollectionDescription`

- <a id="s-8498de7f6d"></a>`type`: `"string"`
- <a id="s-4a33789ab3"></a>`maxLength`: `32768`
- <a id="s-a05f400a6c"></a>`minLength`: `1`
- <a id="s-42d70c3ef4"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-c4611c949e"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-ef746e7815"></a>`x-unicode-normalization`: `"NFC"`

### <a id="s-436bd9bf7b"></a>definition `CollectionTag`

- <a id="s-7f47fe0247"></a>`type`: `"string"`
- <a id="s-c103e9bd1d"></a>`maxLength`: `65536`
- <a id="s-cc97954e02"></a>`minLength`: `1`
- <a id="s-b92ad0b1f4"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-13d06c152e"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-2a545a1146"></a>`x-unicode-normalization`: `"NFC"`

### <a id="s-2ff1393c2b"></a>definition `SourceConfig`

- <a id="s-e9616f3b88"></a>`type`: `"object"`
- <a id="s-827d698992"></a>`additionalProperties`: `false`
- <a id="s-50ef287f96"></a>`description`: `"One deployment-owned, content-opaque intake source."`
- <a id="s-ebb529643a"></a>`required`: `["id","root","ingest_source"]`
- <a id="s-c1677804b8"></a>`title`: `"SourceConfig"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0bb4895494"></a>`archive_store` | no | anyOf=[(type="string"; maxLength=160; minLength=1); (type="null")]; default=null; title="Archive Store" |  |
| <a id="s-222567f8f8"></a>`close_mode` | no | type="string"; enum=["stable","explicit-flush"]; default="stable"; title="Close Mode" |  |
| <a id="s-145d1b7e52"></a>`description` | no | anyOf=[([CollectionDescription](#s-a6bdda00e8)); (type="null")]; default=null |  |
| <a id="s-22f2001f12"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Id" |  |
| <a id="s-2aac5e9410"></a>`ingest_source` | yes | type="string"; maxLength=512; minLength=1; title="Ingest Source" |  |
| <a id="s-e01a0c6eb5"></a>`max_bytes` | no | type="integer"; minimum=1; default=107374182400; title="Max Bytes" |  |
| <a id="s-1ad7230193"></a>`max_files` | no | type="integer"; minimum=1; default=1000; title="Max Files" |  |
| <a id="s-63c64f1086"></a>`provenance` | no | type="string"; enum=["capture","omit"]; default="capture"; title="Provenance" |  |
| <a id="s-a1f7100a24"></a>`provenance_omission_reason` | no | anyOf=[(type="string"; maxLength=1000); (type="null")]; default=null; title="Provenance Omission Reason" |  |
| <a id="s-12e6176a81"></a>`root` | yes | type="string"; format="path"; title="Root" |  |
| <a id="s-0251cac413"></a>`tags` | no | type="array"; default=[]; items=([CollectionTag](#s-436bd9bf7b)); title="Tags" |  |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-ftp-spool:configuration:ftp-spool-config"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition SourceConfig · field tags](#s-0251cac413) | `cardinality · items · operational_policy` | shared above |
| [field sources](#s-b1bab85334) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition CollectionDescription](#s-a6bdda00e8) | `encoded-size · bytes · contract_max` | maximum=32768; reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionDescription](#s-a6bdda00e8) | `length · characters · contract_max` | maximum=32768; minimum=1; reason="schema-maximum" |
| [definition CollectionTag](#s-436bd9bf7b) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionTag](#s-436bd9bf7b) | `length · characters · contract_max` | maximum=65536; minimum=1; reason="schema-maximum" |
| <a id="s-c322f147c5"></a>[definition SourceConfig · field archive_store · string value](#s-0bb4895494) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [definition SourceConfig · field ingest_source](#s-2aac5e9410) | `length · characters · contract_max` | maximum=512; minimum=1; reason="schema-maximum" |
| <a id="s-3674a31f5a"></a>[definition SourceConfig · field provenance_omission_reason · string value](#s-a1f7100a24) | `length · characters · contract_max` | maximum=1000; reason="schema-maximum" |
| [field api_token](#s-15550dd9ff) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field host_id](#s-bad31974cd) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field poll_seconds](#s-b30dc7fc71) | `value · schema-value · contract_max` | maximum=3600; minimum=0.1; reason="schema-maximum" |
| <a id="s-61b269fbc3"></a>[field provenance_observer · string value](#s-06296f46ed) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field riverhog_base_url](#s-9c7f62aa90) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |
| [field riverhog_token](#s-b5fcc83070) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-5b98fcfb98"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-dd900c11ef"></a>[extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)
- <a id="pa-e70c151a7c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:a-riverhog-ftp-spool:configuration:ftp-spool-config](../../../evidence/sources/authorities.md#src-3c878da241) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/config.py::FtpSpoolConfig](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/a-riverhog-ftp-spool:configuration:ftp-spool-config`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f253a046b1ff458a112798fe0f9d9576f0f3e40882f0ff8eea571ed3f99160ac -->

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
  "properties": {
    "allow_insecure_http": {
      "default": false,
      "title": "Allow Insecure Http",
      "type": "boolean"
    },
    "api_token": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Api Token",
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
    "riverhog_token": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Riverhog Token",
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
    "riverhog_token",
    "api_token",
    "sources"
  ],
  "title": "FtpSpoolConfig",
  "type": "object"
}
```

</details>
