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

- <a id="s-302a807ffd"></a>`type`: `"object"`
- <a id="s-1de05ff0c2"></a>`additionalProperties`: `false`
- <a id="s-b4ff706dba"></a>`required`: `["host_id","riverhog_base_url","riverhog_token","api_token","sources"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5045d06e0e"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-6d88a4e326"></a>`api_token` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-36c0d05a01"></a>`claim_attempt_budget` | no | type="integer"; minimum=2; default=8 |  |
| <a id="s-fc31ff2b17"></a>`completion_failure_attempt_budget` | no | type="integer"; minimum=1; default=8 |  |
| <a id="s-3b0b613f4e"></a>`completion_failure_capacity` | no | type="integer"; minimum=1; default=128 |  |
| <a id="s-ac67af580b"></a>`discovery_entry_budget` | no | type="integer"; minimum=1; default=4096 |  |
| <a id="s-5d31aa1e1a"></a>`host_id` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-91b6947ad7"></a>`pending_claim_capacity` | no | type="integer"; minimum=1; default=128 |  |
| <a id="s-a7a28d229f"></a>`poll_seconds` | no | type="number"; minimum=0.1; maximum=3600; default=5 |  |
| <a id="s-2623c96319"></a>`provenance_observer` | no | anyOf=(type="string"; maxLength=255; minLength=1) \| (type="null"); default=null |  |
| <a id="s-594fdb3083"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-18194d3168"></a>`riverhog_token` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-da574a41bd"></a>`sources` | yes | type="array"; items=([SourceConfig](#s-6f4e2328f8)); minItems=1 |  |

##### Definitions

- [CollectionDescription](#s-a303d12867)
- [CollectionTag](#s-6ffee2fa7d)
- [SourceConfig](#s-6f4e2328f8)

##### <a id="s-a303d12867"></a>definition `CollectionDescription`

- <a id="s-be76afd14e"></a>`type`: `"string"`
- <a id="s-0e9f4da453"></a>`maxLength`: `32768`
- <a id="s-18ecbcdc15"></a>`minLength`: `1`
- <a id="s-840e48b4ed"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-d37933a304"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-03f4c06ec2"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-6ffee2fa7d"></a>definition `CollectionTag`

- <a id="s-de917d7bb0"></a>`type`: `"string"`
- <a id="s-d2846d9568"></a>`maxLength`: `65536`
- <a id="s-9160a1cf85"></a>`minLength`: `1`
- <a id="s-8b7fb880bf"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-19047feb4f"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-c0b450e17d"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-6f4e2328f8"></a>definition `SourceConfig`

- <a id="s-c3b2cddfb6"></a>`type`: `"object"`
- <a id="s-df261edf84"></a>`additionalProperties`: `false`
- <a id="s-f02770498c"></a>`required`: `["id","root","ingest_source"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-39a6df08fd"></a>`archive_store` | no | anyOf=(type="string"; maxLength=160; minLength=1) \| (type="null"); default=null |  |
| <a id="s-fa1c0bb167"></a>`close_mode` | no | type="string"; enum=["stable","explicit-flush"]; default="stable" |  |
| <a id="s-57e116d3a3"></a>`description` | no | anyOf=([CollectionDescription](#s-a303d12867)) \| (type="null"); default=null |  |
| <a id="s-d7cd2ad6b4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |
| <a id="s-fc7fd41ff3"></a>`ingest_source` | yes | type="string"; maxLength=512; minLength=1 |  |
| <a id="s-7c89c77ec7"></a>`max_bytes` | no | type="integer"; minimum=1; default=107374182400 |  |
| <a id="s-83c5fde40a"></a>`max_files` | no | type="integer"; minimum=1; default=1000 |  |
| <a id="s-7bb9d9e6ee"></a>`provenance` | no | type="string"; enum=["capture","omit"]; default="capture" |  |
| <a id="s-dd349f1180"></a>`provenance_omission_reason` | no | anyOf=(type="string"; maxLength=1000) \| (type="null"); default=null |  |
| <a id="s-dd2e0ae6f4"></a>`root` | yes | type="string"; format="path" |  |
| <a id="s-ebb34feebc"></a>`tags` | no | type="array"; default=[]; items=([CollectionTag](#s-6ffee2fa7d)) |  |

## Maintained corroboration

### Related interface records

- [provenance_authority](riverhog-ftp-adapter-ftpadapterconfig-provenance-authority.md)
- [source](riverhog-ftp-adapter-ftpadapterconfig-source.md)
- [unique_sources](riverhog-ftp-adapter-ftpadapterconfig-unique-sources.md)

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

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b303f265fc70c0636f8c2452c21be67eabe8d5b79b5b7644d771b1b3e8fda41f -->

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
              "default": null
            },
            "close_mode": {
              "default": "stable",
              "enum": [
                "stable",
                "explicit-flush"
              ],
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
              "type": "string"
            },
            "ingest_source": {
              "maxLength": 512,
              "minLength": 1,
              "type": "string"
            },
            "max_bytes": {
              "default": 107374182400,
              "minimum": 1,
              "type": "integer"
            },
            "max_files": {
              "default": 1000,
              "minimum": 1,
              "type": "integer"
            },
            "provenance": {
              "default": "capture",
              "enum": [
                "capture",
                "omit"
              ],
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
              "default": null
            },
            "root": {
              "format": "path",
              "type": "string"
            },
            "tags": {
              "default": [],
              "items": {
                "$ref": "#/$defs/CollectionTag"
              },
              "type": "array"
            }
          },
          "required": [
            "id",
            "root",
            "ingest_source"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "type": "boolean"
        },
        "api_token": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "claim_attempt_budget": {
          "default": 8,
          "minimum": 2,
          "type": "integer"
        },
        "completion_failure_attempt_budget": {
          "default": 8,
          "minimum": 1,
          "type": "integer"
        },
        "completion_failure_capacity": {
          "default": 128,
          "minimum": 1,
          "type": "integer"
        },
        "discovery_entry_budget": {
          "default": 4096,
          "minimum": 1,
          "type": "integer"
        },
        "host_id": {
          "maxLength": 255,
          "minLength": 1,
          "type": "string"
        },
        "pending_claim_capacity": {
          "default": 128,
          "minimum": 1,
          "type": "integer"
        },
        "poll_seconds": {
          "default": 5,
          "maximum": 3600,
          "minimum": 0.1,
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
          "default": null
        },
        "riverhog_base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "type": "string"
        },
        "riverhog_token": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "sources": {
          "items": {
            "$ref": "#/$defs/SourceConfig"
          },
          "minItems": 1,
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

</details>
