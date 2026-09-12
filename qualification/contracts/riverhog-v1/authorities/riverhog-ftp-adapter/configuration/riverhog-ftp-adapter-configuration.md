# riverhog-ftp-adapter configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:riverhog-ftp-adapter:riverhog-ftp-adapter-configuration:e46af825b1 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `configuration` |
| Family | `documents` |
| Contract elements | 1 |
| Extent decisions | 15 |

## Machine authority

- `/external_contract/configuration_documents/riverhog-ftp-adapter`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configuration-composition/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `configuration:riverhog-ftp-adapter` — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py::FtpAdapterConfig`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| encoded-size | bytes | `contract_max` | maximum=32768, reason=bounded-human-authored-catalog-description |
| length | characters | `contract_max` | maximum=32768, minimum=1, reason=schema-maximum |
| encoded-size | bytes | `contract_max` | maximum=65536, reason=bounded-human-authored-collection-tag |
| length | characters | `contract_max` | maximum=65536, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=512, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=1000, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=255, minimum=1, reason=schema-maximum |
| value | schema-value | `contract_max` | maximum=3600, minimum=0.1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=255, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=2048, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |

## Contract summary

- `title`: FtpAdapterConfig
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `allow_insecure_http` | no | boolean |  |
| `api_token` | yes | string |  |
| `claim_attempt_budget` | no | integer |  |
| `completion_failure_attempt_budget` | no | integer |  |
| `completion_failure_capacity` | no | integer |  |
| `discovery_entry_budget` | no | integer |  |
| `host_id` | yes | string |  |
| `pending_claim_capacity` | no | integer |  |
| `poll_seconds` | no | number |  |
| `provenance_observer` | no | object (3 fields) |  |
| `riverhog_base_url` | yes | string |  |
| `riverhog_token` | yes | string |  |
| `sources` | yes | array |  |

### Definitions

| Definition | Shape |
|---|---|
| `CollectionDescription` | string |
| `CollectionTag` | string |
| `SourceConfig` | object |

## Complete owned contract

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
