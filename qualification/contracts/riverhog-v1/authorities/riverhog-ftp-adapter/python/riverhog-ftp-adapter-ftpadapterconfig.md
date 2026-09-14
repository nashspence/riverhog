# riverhog_ftp_adapter.FtpAdapterConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-ftpadapterconfig:c98ed5305f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c8cff4a0dd"></a>
- <a id="s-5d1a9a7079"></a>`distribution`: `riverhog-ftp-adapter`
- <a id="s-a0c2b9e7b5"></a>`module`: `riverhog_ftp_adapter`
- <a id="s-dccfd93994"></a>`name`: `FtpAdapterConfig`
- <a id="s-070e05ce3d"></a>`unit`: `export`

### Declared structure

- <a id="s-6bcb71978e"></a>`kind`: `"class"`
- <a id="s-562346b738"></a>`signature`: `"'(*, host_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], riverhog_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False, api_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], provenance_observer: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=255)] = None, sources: Annotated[tuple[riverhog_ftp_adapter.config.SourceConfig, ...], MinLen(min_length=1)], poll_seconds: Annotated[float, Ge(ge=0.1), Le(le=3600)] = 5.0, pending_claim_capacity: Annotated[int, Ge(ge=1)] = 128, claim_attempt_budget: Annotated[int, Ge(ge=2)] = 8, discovery_entry_budget: Annotated[int, Ge(ge=1)] = 4096, completion_failure_capacity: Annotated[int, Ge(ge=1)] = 128, completion_failure_attempt_budget: Annotated[int, Ge(ge=1)] = 8) -> None'"`

#### Validated model schema

<a id="s-35f66ae3e4"></a>
- <a id="s-71aa6dc289"></a>`title`: FtpAdapterConfig
- <a id="s-302a807ffd"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5045d06e0e"></a>`allow_insecure_http` | no | type="boolean" |  |
| <a id="s-6d88a4e326"></a>`api_token` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-36c0d05a01"></a>`claim_attempt_budget` | no | type="integer"; minimum=2 |  |
| <a id="s-fc31ff2b17"></a>`completion_failure_attempt_budget` | no | type="integer"; minimum=1 |  |
| <a id="s-3b0b613f4e"></a>`completion_failure_capacity` | no | type="integer"; minimum=1 |  |
| <a id="s-ac67af580b"></a>`discovery_entry_budget` | no | type="integer"; minimum=1 |  |
| <a id="s-5d31aa1e1a"></a>`host_id` | yes | type="string"; minLength=1; maxLength=255 |  |
| <a id="s-91b6947ad7"></a>`pending_claim_capacity` | no | type="integer"; minimum=1 |  |
| <a id="s-a7a28d229f"></a>`poll_seconds` | no | type="number"; minimum=0.1; maximum=3600 |  |
| <a id="s-2623c96319"></a>`provenance_observer` | no | anyOf=type="string"; minLength=1; maxLength=255 \| type="null" |  |
| <a id="s-594fdb3083"></a>`riverhog_base_url` | yes | type="string"; minLength=1; maxLength=2048 |  |
| <a id="s-18194d3168"></a>`riverhog_token` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-da574a41bd"></a>`sources` | yes | type="array"; minItems=1; items=(#/$defs/SourceConfig) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-a303d12867"></a>`CollectionDescription` | type="string"; minLength=1; maxLength=32768; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-6ffee2fa7d"></a>`CollectionTag` | type="string"; minLength=1; maxLength=65536; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-6f4e2328f8"></a>`SourceConfig` | type="object"; fields=`archive_store`, `close_mode`, `description`, `id`, `ingest_source`, `max_bytes`, `max_files`, `provenance`, `provenance_omission_reason`, `root`, `tags`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [riverhog_ftp_adapter.FtpAdapterConfig.provenance_authority](riverhog-ftp-adapter-ftpadapterconfig-provenance-authority.md)
- [riverhog_ftp_adapter.FtpAdapterConfig.source](riverhog-ftp-adapter-ftpadapterconfig-source.md)
- [riverhog_ftp_adapter.FtpAdapterConfig.unique_sources](riverhog-ftp-adapter-ftpadapterconfig-unique-sources.md)

## Governing policies

- <a id="pa-9dca47410a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.FtpAdapterConfig`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b7352d2df94995c6bbd7105dcdb980ce868c3d9499d7f6908f3063ff3547f45 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
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
    },
    "signature": "'(*, host_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], riverhog_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False, api_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], provenance_observer: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None, sources: Annotated[tuple[riverhog_ftp_adapter.config.SourceConfig, ...], MinLen(min_length=1)], poll_seconds: Annotated[float, Ge(ge=0.1), Le(le=3600)] = 5.0, pending_claim_capacity: Annotated[int, Ge(ge=1)] = 128, claim_attempt_budget: Annotated[int, Ge(ge=2)] = 8, discovery_entry_budget: Annotated[int, Ge(ge=1)] = 4096, completion_failure_capacity: Annotated[int, Ge(ge=1)] = 128, completion_failure_attempt_budget: Annotated[int, Ge(ge=1)] = 8) -> None'"
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "FtpAdapterConfig",
  "unit": "export"
}
```
