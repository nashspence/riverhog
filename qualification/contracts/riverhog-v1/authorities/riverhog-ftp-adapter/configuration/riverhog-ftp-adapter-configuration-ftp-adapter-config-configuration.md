# riverhog-ftp-adapter:configuration:ftp-adapter-config configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:riverhog-ftp-adapter:riverhog-ftp-adapter-configuration-ftp-ad-b7d9b2cbf8:5c5d6022e9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-1720d3961c"></a>
- <a id="s-16e59a37de"></a>`title`: FtpAdapterConfig
- <a id="s-fe9ddcb15a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f911dd1f94"></a>`allow_insecure_http` | no | type="boolean" |  |
| <a id="s-dbf5ea4ac5"></a>`api_token` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-7c7fe84c8b"></a>`claim_attempt_budget` | no | type="integer"; minimum=2 |  |
| <a id="s-684f7aa9c4"></a>`completion_failure_attempt_budget` | no | type="integer"; minimum=1 |  |
| <a id="s-c151434722"></a>`completion_failure_capacity` | no | type="integer"; minimum=1 |  |
| <a id="s-6c890fc5d5"></a>`discovery_entry_budget` | no | type="integer"; minimum=1 |  |
| <a id="s-72ad5db1e6"></a>`host_id` | yes | type="string"; minLength=1; maxLength=255 |  |
| <a id="s-7c5fa78bfc"></a>`pending_claim_capacity` | no | type="integer"; minimum=1 |  |
| <a id="s-82aa3eb24d"></a>`poll_seconds` | no | type="number"; minimum=0.1; maximum=3600 |  |
| <a id="s-2a409954ca"></a>`provenance_observer` | no | anyOf=type="string"; minLength=1; maxLength=255 \| type="null" |  |
| <a id="s-22a68b6f37"></a>`riverhog_base_url` | yes | type="string"; minLength=1; maxLength=2048 |  |
| <a id="s-132c5582fa"></a>`riverhog_token` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-c96e3a57ab"></a>`sources` | yes | type="array"; minItems=1; items=(#/$defs/SourceConfig) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-de0b3f6676"></a>`CollectionDescription` | type="string"; minLength=1; maxLength=32768; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-031bfd2553"></a>`CollectionTag` | type="string"; minLength=1; maxLength=65536; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-b4cbe4146f"></a>`SourceConfig` | type="object"; fields=`archive_store`, `close_mode`, `description`, `id`, `ingest_source`, `max_bytes`, `max_files`, `provenance`, `provenance_omission_reason`, `root`, `tags`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-ftp-adapter:configuration:ftp-adapter-config"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-779c3d2f99"></a>[definition SourceConfig · field tags](#s-b4cbe4146f) | `cardinality · items · operational_policy` | shared above |
| [field sources](#s-c96e3a57ab) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition CollectionDescription](#s-de0b3f6676) | `encoded-size · bytes · contract_max` | maximum=32768; reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionDescription](#s-de0b3f6676) | `length · characters · contract_max` | maximum=32768; minimum=1; reason="schema-maximum" |
| [definition CollectionTag](#s-031bfd2553) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionTag](#s-031bfd2553) | `length · characters · contract_max` | maximum=65536; minimum=1; reason="schema-maximum" |
| <a id="s-58715b7340"></a>[definition SourceConfig · field archive_store · string value](#s-b4cbe4146f) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| <a id="s-d364f97555"></a>[definition SourceConfig · field ingest_source](#s-b4cbe4146f) | `length · characters · contract_max` | maximum=512; minimum=1; reason="schema-maximum" |
| <a id="s-6e40570cee"></a>[definition SourceConfig · field provenance_omission_reason · string value](#s-b4cbe4146f) | `length · characters · contract_max` | maximum=1000; reason="schema-maximum" |
| [field api_token](#s-dbf5ea4ac5) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field host_id](#s-72ad5db1e6) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field poll_seconds](#s-82aa3eb24d) | `value · schema-value · contract_max` | maximum=3600; minimum=0.1; reason="schema-maximum" |
| <a id="s-292da78a97"></a>[field provenance_observer · string value](#s-2a409954ca) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field riverhog_base_url](#s-22a68b6f37) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |
| [field riverhog_token](#s-132c5582fa) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-80f8050bfa"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-9634b80f62"></a>[extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)
- <a id="pa-91e400356e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration:riverhog-ftp-adapter:configuration:ftp-adapter-config](../../../evidence/sources.md#src-cf8c44826f) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py::FtpAdapterConfig`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/riverhog-ftp-adapter:configuration:ftp-adapter-config`

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
