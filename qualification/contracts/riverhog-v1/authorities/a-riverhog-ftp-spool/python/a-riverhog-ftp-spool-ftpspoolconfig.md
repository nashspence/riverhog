# a_riverhog_ftp_spool.FtpSpoolConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-ftpspoolconfig:e464ec01eb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-94e1b43f40"></a>
- <a id="s-dd1e2fc19b"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-cf806eba69"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-7247bf1f2e"></a>`name`: `FtpSpoolConfig`
- <a id="s-199248c766"></a>`unit`: `export`

### Declared structure

- <a id="s-05b7ae6f2b"></a>`kind`: `"class"`
- <a id="s-46e7af5f1f"></a>`signature`: `"'(*, host_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], riverhog_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False, api_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], provenance_observer: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=255)] = None, sources: Annotated[tuple[a_riverhog_ftp_spool.config.SourceConfig, ...], MinLen(min_length=1)], poll_seconds: Annotated[float, Ge(ge=0.1), Le(le=3600)] = 5.0, pending_claim_capacity: Annotated[int, Ge(ge=1)] = 128, claim_attempt_budget: Annotated[int, Ge(ge=2)] = 8, discovery_entry_budget: Annotated[int, Ge(ge=1)] = 4096, completion_failure_capacity: Annotated[int, Ge(ge=1)] = 128, completion_failure_attempt_budget: Annotated[int, Ge(ge=1)] = 8) -> None'"`

#### Validated model schema

<a id="s-14ceb68bfa"></a>

- <a id="s-17cf68c0e7"></a>`type`: `"object"`
- <a id="s-afa8e29886"></a>`additionalProperties`: `false`
- <a id="s-6649691b3a"></a>`required`: `["host_id","riverhog_base_url","riverhog_token","api_token","sources"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2a79b57c0a"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-39673e4556"></a>`api_token` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-d952be6163"></a>`claim_attempt_budget` | no | type="integer"; minimum=2; default=8 |  |
| <a id="s-1b1b67499f"></a>`completion_failure_attempt_budget` | no | type="integer"; minimum=1; default=8 |  |
| <a id="s-8833ff395a"></a>`completion_failure_capacity` | no | type="integer"; minimum=1; default=128 |  |
| <a id="s-f3a5382048"></a>`discovery_entry_budget` | no | type="integer"; minimum=1; default=4096 |  |
| <a id="s-cc800c66ee"></a>`host_id` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-6545bb0c1f"></a>`pending_claim_capacity` | no | type="integer"; minimum=1; default=128 |  |
| <a id="s-902234a777"></a>`poll_seconds` | no | type="number"; minimum=0.1; maximum=3600; default=5 |  |
| <a id="s-f1fa429994"></a>`provenance_observer` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-a7f831ba89"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-08f2bb5f63"></a>`riverhog_token` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-3a070ed225"></a>`sources` | yes | type="array"; items=([SourceConfig](#s-460987e5cc)); minItems=1 |  |

##### Definitions

- [CollectionDescription](#s-b81d3d46c3)
- [CollectionTag](#s-042348bdc4)
- [SourceConfig](#s-460987e5cc)

##### <a id="s-b81d3d46c3"></a>definition `CollectionDescription`

- <a id="s-6d8c172a71"></a>`type`: `"string"`
- <a id="s-4a4bde62b0"></a>`maxLength`: `32768`
- <a id="s-508177827c"></a>`minLength`: `1`
- <a id="s-d085266ccd"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-fb52d8ed06"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-ba38b090dd"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-042348bdc4"></a>definition `CollectionTag`

- <a id="s-9e5208b6bb"></a>`type`: `"string"`
- <a id="s-7ec35b6c51"></a>`maxLength`: `65536`
- <a id="s-4809b62e7b"></a>`minLength`: `1`
- <a id="s-0a5b84c92e"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-854cd10b4b"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-f0512e27b0"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-460987e5cc"></a>definition `SourceConfig`

- <a id="s-f419c865a4"></a>`type`: `"object"`
- <a id="s-026091ba2c"></a>`additionalProperties`: `false`
- <a id="s-989f4b0711"></a>`required`: `["id","root","ingest_source"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-70e992d8cd"></a>`archive_store` | no | anyOf=[(type="string"; maxLength=160; minLength=1); (type="null")]; default=null |  |
| <a id="s-75a8bfb96b"></a>`close_mode` | no | type="string"; enum=["stable","explicit-flush"]; default="stable" |  |
| <a id="s-4898219fe7"></a>`description` | no | anyOf=[([CollectionDescription](#s-b81d3d46c3)); (type="null")]; default=null |  |
| <a id="s-5d952ea5b4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |
| <a id="s-4df52edce6"></a>`ingest_source` | yes | type="string"; maxLength=512; minLength=1 |  |
| <a id="s-efeadf1acd"></a>`max_bytes` | no | type="integer"; minimum=1; default=107374182400 |  |
| <a id="s-e8fcf25546"></a>`max_files` | no | type="integer"; minimum=1; default=1000 |  |
| <a id="s-865ea5e03d"></a>`provenance` | no | type="string"; enum=["capture","omit"]; default="capture" |  |
| <a id="s-81f66c843b"></a>`provenance_omission_reason` | no | anyOf=[(type="string"; maxLength=1000); (type="null")]; default=null |  |
| <a id="s-d1cb1adc0c"></a>`root` | yes | type="string"; format="path" |  |
| <a id="s-cd866ec8cf"></a>`tags` | no | type="array"; default=[]; items=([CollectionTag](#s-042348bdc4)) |  |

## Maintained corroboration

### Related interface records

- [provenance_authority](a-riverhog-ftp-spool-ftpspoolconfig-provenance-authority.md)
- [source](a-riverhog-ftp-spool-ftpspoolconfig-source.md)
- [unique_sources](a-riverhog-ftp-spool-ftpspoolconfig-unique-sources.md)

## Governing policies

- <a id="pa-2e29a74f59"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.FtpSpoolConfig`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e7b5ff1c65ef0f3c807302ec3f6dda8db2921968fe69487e00d12d1ca27f134 -->

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
    "signature": "'(*, host_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], riverhog_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False, api_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], provenance_observer: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None, sources: Annotated[tuple[a_riverhog_ftp_spool.config.SourceConfig, ...], MinLen(min_length=1)], poll_seconds: Annotated[float, Ge(ge=0.1), Le(le=3600)] = 5.0, pending_claim_capacity: Annotated[int, Ge(ge=1)] = 128, claim_attempt_budget: Annotated[int, Ge(ge=2)] = 8, discovery_entry_budget: Annotated[int, Ge(ge=1)] = 4096, completion_failure_capacity: Annotated[int, Ge(ge=1)] = 128, completion_failure_attempt_budget: Annotated[int, Ge(ge=1)] = 8) -> None'"
  },
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "FtpSpoolConfig",
  "unit": "export"
}
```

</details>
