# riverhog-ftp-adapter configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:riverhog-ftp-adapter:riverhog-ftp-adapter-configuration:e46af825b1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [configuration](index.md) |
| Family | [documents](index.md#f-33ea702762b5) |
| Contract elements | 1 |
| Extent decisions | 15 |

## External contract

<a id="s-feeab7e17f2f"></a>
- <a id="s-516d254f286c"></a>`title`: FtpAdapterConfig
- <a id="s-68e742692f77"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e8b73fd9ca6f"></a>`allow_insecure_http` | no | type="boolean" |  |
| <a id="s-1db52df8b42e"></a>`api_token` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-bf710ca4606f"></a>`claim_attempt_budget` | no | type="integer"; minimum=2 |  |
| <a id="s-99752e5a7476"></a>`completion_failure_attempt_budget` | no | type="integer"; minimum=1 |  |
| <a id="s-38c21a889678"></a>`completion_failure_capacity` | no | type="integer"; minimum=1 |  |
| <a id="s-83331046c2e9"></a>`discovery_entry_budget` | no | type="integer"; minimum=1 |  |
| <a id="s-29c0e9cadfda"></a>`host_id` | yes | type="string"; minLength=1; maxLength=255 |  |
| <a id="s-6833b78c492b"></a>`pending_claim_capacity` | no | type="integer"; minimum=1 |  |
| <a id="s-f2994bcc3fc9"></a>`poll_seconds` | no | type="number"; minimum=0.1; maximum=3600 |  |
| <a id="s-ad2ed195aa09"></a>`provenance_observer` | no | anyOf=type="string"; minLength=1; maxLength=255 \| type="null" |  |
| <a id="s-6552338bd072"></a>`riverhog_base_url` | yes | type="string"; minLength=1; maxLength=2048 |  |
| <a id="s-5d6a299ec4fc"></a>`riverhog_token` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-5bd17d2ec6b8"></a>`sources` | yes | type="array"; minItems=1; items=(#/$defs/SourceConfig) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-3675001ccbd4"></a>`CollectionDescription` | type="string"; minLength=1; maxLength=32768; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-200a55177271"></a>`CollectionTag` | type="string"; minLength=1; maxLength=65536; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-319a1b8d8c78"></a>`SourceConfig` | type="object"; fields=`archive_store`, `close_mode`, `description`, `id`, `ingest_source`, `max_bytes`, `max_files`, `provenance`, `provenance_omission_reason`, `root`, `tags`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e519)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-ftp-adapter"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-02f98a374099"></a>definition SourceConfig · field tags | `cardinality · items · operational_policy` | shared above |
| [field sources](#s-5bd17d2ec6b8) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition CollectionDescription](#s-3675001ccbd4) | `encoded-size · bytes · contract_max` | maximum=32768; reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionDescription](#s-3675001ccbd4) | `length · characters · contract_max` | maximum=32768; minimum=1; reason="schema-maximum" |
| [definition CollectionTag](#s-200a55177271) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionTag](#s-200a55177271) | `length · characters · contract_max` | maximum=65536; minimum=1; reason="schema-maximum" |
| <a id="s-29f9d1c169e3"></a>definition SourceConfig · field archive_store · anyOf alternative 1 | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| <a id="s-985ad3446d25"></a>definition SourceConfig · field ingest_source | `length · characters · contract_max` | maximum=512; minimum=1; reason="schema-maximum" |
| <a id="s-0387f9a7a477"></a>definition SourceConfig · field provenance_omission_reason · anyOf alternative 1 | `length · characters · contract_max` | maximum=1000; reason="schema-maximum" |
| [field api_token](#s-1db52df8b42e) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field host_id](#s-29c0e9cadfda) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field poll_seconds](#s-f2994bcc3fc9) | `value · schema-value · contract_max` | maximum=3600; minimum=0.1; reason="schema-maximum" |
| <a id="s-ce2b5c5fee66"></a>field provenance_observer · anyOf alternative 1 | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field riverhog_base_url](#s-6552338bd072) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |
| [field riverhog_token](#s-5d6a299ec4fc) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-6e7185101ee6"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-a9de2663abe0"></a>[extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e519)
- <a id="pa-98727f523e11"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration:riverhog-ftp-adapter](../../../evidence/sources.md#src-4d54b4c8a0ea) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py::FtpAdapterConfig`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/riverhog-ftp-adapter`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf4fc0499f8c7d0a5706676b673ebd209f6fa203de84b35c475d3ed7f4add88b -->

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
  "title": "FtpAdapterConfig",
  "type": "object"
}
```
