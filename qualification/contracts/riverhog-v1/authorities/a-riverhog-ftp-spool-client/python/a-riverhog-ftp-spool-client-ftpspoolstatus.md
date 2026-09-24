# a_riverhog_ftp_spool_client.FtpSpoolStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-ftpspoolstatus:3fd2e6a6e8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f9c2c14de"></a>
- <a id="s-62c244763a"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-97d64c5a7d"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-f984045c02"></a>`name`: `FtpSpoolStatus`
- <a id="s-b919f34099"></a>`unit`: `export`

### Declared structure

- <a id="s-cd19d74836"></a>`kind`: `"class"`
- <a id="s-9609522fbf"></a>`signature`: `"\"(*, format: Literal['a-riverhog-ftp-spool-status/v1'], provenance_observer: str \| None, sources: Annotated[list[a_riverhog_ftp_spool_client.status.SourceStatus], MaxLen(max_length=100)], page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken \| None, snapshot: Literal[False]) -> None\""`

#### Validated model schema

<a id="s-901cfeb090"></a>

- <a id="s-28cb51309f"></a>`type`: `"object"`
- <a id="s-40c19173b4"></a>`additionalProperties`: `false`
- <a id="s-1fe017a6e1"></a>`required`: `["format","provenance_observer","sources","page_size","next_page_token","snapshot"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a1be15e1ab"></a>`format` | yes | type="string"; const="a-riverhog-ftp-spool-status/v1" |  |
| <a id="s-f9ee9b6fa4"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](#s-d8d84900f6)); (type="null")] |  |
| <a id="s-4ce9188a40"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-89f4c323ca"></a>`provenance_observer` | yes | anyOf=[(type="string"); (type="null")] |  |
| <a id="s-9f37cb7d22"></a>`snapshot` | yes | type="boolean"; const=false |  |
| <a id="s-55c27486c0"></a>`sources` | yes | type="array"; items=([SourceStatus](#s-965f6ea821)); maxItems=100 |  |

##### Definitions

- [BrowsePageToken](#s-d8d84900f6)
- [CompletionFailureStatus](#s-f0f64877e3)
- [SourceStatus](#s-965f6ea821)

##### <a id="s-d8d84900f6"></a>definition `BrowsePageToken`

- <a id="s-bf74bda1ed"></a>`type`: `"string"`
- <a id="s-8b19384dc2"></a>`maxLength`: `8192`
- <a id="s-9e87dc501a"></a>`minLength`: `1`

##### <a id="s-f0f64877e3"></a>definition `CompletionFailureStatus`

- <a id="s-70e49e3469"></a>`type`: `"object"`
- <a id="s-1f42f80389"></a>`additionalProperties`: `false`
- <a id="s-f304f2f5f3"></a>`required`: `["reason","retryable","attempts"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-424709e26c"></a>`attempts` | yes | type="integer"; minimum=0 |  |
| <a id="s-4c6d1dbec4"></a>`reason` | yes | type="string" |  |
| <a id="s-897e98ad9d"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-965f6ea821"></a>definition `SourceStatus`

- <a id="s-2c0cd385b2"></a>`type`: `"object"`
- <a id="s-ce45d8adb1"></a>`additionalProperties`: `false`
- <a id="s-a2187843be"></a>`required`: `["id","ingest_source","claims","claim_bytes","close_mode","max_files","max_bytes","provenance","pending_claim_capacity","completion_failures","completion_failure_capacity","oldest_completion_failure"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-072d172e6c"></a>`claim_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-c29b195b25"></a>`claims` | yes | type="integer"; minimum=0 |  |
| <a id="s-66050a63b8"></a>`close_mode` | yes | type="string"; enum=["stable","explicit-flush"] |  |
| <a id="s-706c67e7d4"></a>`completion_failure_capacity` | yes | type="integer"; minimum=1 |  |
| <a id="s-3c9a575b35"></a>`completion_failures` | yes | type="integer"; minimum=0 |  |
| <a id="s-b70aa1c091"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |
| <a id="s-2d40cc216f"></a>`ingest_source` | yes | type="string" |  |
| <a id="s-18162e1107"></a>`max_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-d5a217a876"></a>`max_files` | yes | type="integer"; minimum=1 |  |
| <a id="s-a820a10074"></a>`oldest_completion_failure` | yes | anyOf=[([CompletionFailureStatus](#s-f0f64877e3)); (type="null")] |  |
| <a id="s-c777155e4e"></a>`pending_claim_capacity` | yes | type="integer"; minimum=1 |  |
| <a id="s-22b8bd78d5"></a>`provenance` | yes | type="string"; enum=["capture","omit"] |  |

## Governing policies

- <a id="pa-38e73783b7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.FtpSpoolStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2c6f415fb022c7f9ab8e8caf96b9c69c4dfff786e9cdeef45fa05e873c5e8182 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "BrowsePageToken": {
          "maxLength": 8192,
          "minLength": 1,
          "type": "string"
        },
        "CompletionFailureStatus": {
          "additionalProperties": false,
          "properties": {
            "attempts": {
              "minimum": 0,
              "type": "integer"
            },
            "reason": {
              "type": "string"
            },
            "retryable": {
              "type": "boolean"
            }
          },
          "required": [
            "reason",
            "retryable",
            "attempts"
          ],
          "type": "object"
        },
        "SourceStatus": {
          "additionalProperties": false,
          "properties": {
            "claim_bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "claims": {
              "minimum": 0,
              "type": "integer"
            },
            "close_mode": {
              "enum": [
                "stable",
                "explicit-flush"
              ],
              "type": "string"
            },
            "completion_failure_capacity": {
              "minimum": 1,
              "type": "integer"
            },
            "completion_failures": {
              "minimum": 0,
              "type": "integer"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
              "type": "string"
            },
            "ingest_source": {
              "type": "string"
            },
            "max_bytes": {
              "minimum": 1,
              "type": "integer"
            },
            "max_files": {
              "minimum": 1,
              "type": "integer"
            },
            "oldest_completion_failure": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CompletionFailureStatus"
                },
                {
                  "type": "null"
                }
              ]
            },
            "pending_claim_capacity": {
              "minimum": 1,
              "type": "integer"
            },
            "provenance": {
              "enum": [
                "capture",
                "omit"
              ],
              "type": "string"
            }
          },
          "required": [
            "id",
            "ingest_source",
            "claims",
            "claim_bytes",
            "close_mode",
            "max_files",
            "max_bytes",
            "provenance",
            "pending_claim_capacity",
            "completion_failures",
            "completion_failure_capacity",
            "oldest_completion_failure"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "a-riverhog-ftp-spool-status/v1",
          "type": "string"
        },
        "next_page_token": {
          "anyOf": [
            {
              "$ref": "#/$defs/BrowsePageToken"
            },
            {
              "type": "null"
            }
          ]
        },
        "page_size": {
          "maximum": 100,
          "minimum": 1,
          "type": "integer"
        },
        "provenance_observer": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ]
        },
        "snapshot": {
          "const": false,
          "type": "boolean"
        },
        "sources": {
          "items": {
            "$ref": "#/$defs/SourceStatus"
          },
          "maxItems": 100,
          "type": "array"
        }
      },
      "required": [
        "format",
        "provenance_observer",
        "sources",
        "page_size",
        "next_page_token",
        "snapshot"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['a-riverhog-ftp-spool-status/v1'], provenance_observer: str | None, sources: Annotated[list[a_riverhog_ftp_spool_client.status.SourceStatus], MaxLen(max_length=100)], page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, snapshot: Literal[False]) -> None\""
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "FtpSpoolStatus",
  "unit": "export"
}
```

</details>
