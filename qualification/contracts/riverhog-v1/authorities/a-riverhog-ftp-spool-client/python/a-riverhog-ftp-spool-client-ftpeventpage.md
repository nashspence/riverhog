# a_riverhog_ftp_spool_client.FtpEventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-ftpeventpage:a06edaf8dd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-962884c727"></a>
- <a id="s-42744a668a"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-74db9a512b"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-c6caa2ab45"></a>`name`: `FtpEventPage`
- <a id="s-9d5bcf8a32"></a>`unit`: `export`

### Declared structure

- <a id="s-e5afb27cf7"></a>`kind`: `"class"`
- <a id="s-8c47027e6d"></a>`signature`: `"'(*, events: Annotated[list[FtpLifecycleEvent], MaxLen(max_length=100)], next_cursor: Annotated[str, MinLen(min_length=1), MaxLen(max_length=220)], has_more: bool) -> None'"`

#### Validated model schema

<a id="s-22850c3e8a"></a>

- <a id="s-bf01c5104d"></a>`type`: `"object"`
- <a id="s-d95f3e9197"></a>`additionalProperties`: `false`
- <a id="s-b795cd17e1"></a>`required`: `["events","next_cursor","has_more"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-280079279a"></a>`events` | yes | type="array"; items=([FtpLifecycleEvent](#s-e256f516e3)); maxItems=100 |  |
| <a id="s-b2a5bb629c"></a>`has_more` | yes | type="boolean" |  |
| <a id="s-1ac521ffc1"></a>`next_cursor` | yes | type="string"; maxLength=220; minLength=1 |  |

##### Definitions

- [AttemptFailedEvent](#s-f2414a5286)
- [AttemptFailedPayload](#s-0e8b1d0546)
- [ClaimPublishedEvent](#s-ef52d4b674)
- [ClaimPublishedPayload](#s-92f34f54dc)
- [ClaimRegisteredEvent](#s-870657c4ba)
- [ClaimRegisteredPayload](#s-1bbe28535e)
- [CustodyReadyEvent](#s-2529d76c75)
- [CustodyReadyPayload](#s-8f2eaec6bc)
- [FtpLifecycleEvent](#s-e256f516e3)

##### <a id="s-f2414a5286"></a>definition `AttemptFailedEvent`

- <a id="s-b671e17c4e"></a>`type`: `"object"`
- <a id="s-a76e5e66d8"></a>`additionalProperties`: `false`
- <a id="s-6dcc2e2c02"></a>`required`: `["id","type","subject","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b989decb21"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-c539cd25b4"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-9e9b66f043"></a>`payload` | yes | [AttemptFailedPayload](#s-0e8b1d0546) |  |
| <a id="s-8dfc7262d2"></a>`subject` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a53836683a"></a>`type` | yes | type="string"; const="io.riverhog.ftp_spool.claim.attempt_failed" |  |

##### <a id="s-0e8b1d0546"></a>definition `AttemptFailedPayload`

- <a id="s-4e351eeea4"></a>`type`: `"object"`
- <a id="s-05064fe527"></a>`additionalProperties`: `false`
- <a id="s-05ebfff26e"></a>`required`: `["source_id","claim_id","source_event_id","error_type"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-743821108d"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-012ece7942"></a>`error_type` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-dbf3734b63"></a>`source_event_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-463c6d20f2"></a>`source_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |

##### <a id="s-ef52d4b674"></a>definition `ClaimPublishedEvent`

- <a id="s-d94040d977"></a>`type`: `"object"`
- <a id="s-4f8638dbcb"></a>`additionalProperties`: `false`
- <a id="s-b69fcc2b8a"></a>`required`: `["id","type","subject","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1fa7ba18bc"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-6af6028719"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-11861c5bf1"></a>`payload` | yes | [ClaimPublishedPayload](#s-92f34f54dc) |  |
| <a id="s-f1bcbd8077"></a>`subject` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5c220d8abe"></a>`type` | yes | type="string"; const="io.riverhog.ftp_spool.claim.published" |  |

##### <a id="s-92f34f54dc"></a>definition `ClaimPublishedPayload`

- <a id="s-1b31a7913f"></a>`type`: `"object"`
- <a id="s-820de76a9e"></a>`additionalProperties`: `false`
- <a id="s-2f2293d1d1"></a>`required`: `["source_id","claim_id","source_event_id","collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-693adea116"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-03f37da8b5"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4b0c407bbd"></a>`collection_id` | yes | type="string"; pattern="^[1-9][0-9]*$" |  |
| <a id="s-7bd4cd0fd5"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-092c968e75"></a>`source_event_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-4760df9f72"></a>`source_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |

##### <a id="s-870657c4ba"></a>definition `ClaimRegisteredEvent`

- <a id="s-bdd7d23aa8"></a>`type`: `"object"`
- <a id="s-41c45f7f4f"></a>`additionalProperties`: `false`
- <a id="s-ee17a182f7"></a>`required`: `["id","type","subject","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2ad47ff33f"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-60d7bdbbe3"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-efc5343196"></a>`payload` | yes | [ClaimRegisteredPayload](#s-1bbe28535e) |  |
| <a id="s-68ff0cd623"></a>`subject` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0912cba517"></a>`type` | yes | type="string"; const="io.riverhog.ftp_spool.claim.registered" |  |

##### <a id="s-1bbe28535e"></a>definition `ClaimRegisteredPayload`

- <a id="s-3e24af6ff1"></a>`type`: `"object"`
- <a id="s-5b2a67fdd9"></a>`additionalProperties`: `false`
- <a id="s-9162a5b10a"></a>`required`: `["source_id","claim_id","source_event_id","file_count","bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d825b44d3c"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-aee89c33f2"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-819906449e"></a>`file_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-f88d81704b"></a>`source_event_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-b55607f077"></a>`source_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |

##### <a id="s-2529d76c75"></a>definition `CustodyReadyEvent`

- <a id="s-168d671d4e"></a>`type`: `"object"`
- <a id="s-cb78022016"></a>`additionalProperties`: `false`
- <a id="s-a8a35e7e6d"></a>`required`: `["id","type","subject","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8005797cec"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-18d1057304"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-0d2cf963e7"></a>`payload` | yes | [CustodyReadyPayload](#s-8f2eaec6bc) |  |
| <a id="s-e82db609bc"></a>`subject` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bd53433ad7"></a>`type` | yes | type="string"; const="io.riverhog.ftp_spool.claim.custody_ready" |  |

##### <a id="s-8f2eaec6bc"></a>definition `CustodyReadyPayload`

- <a id="s-a6b3837ee3"></a>`type`: `"object"`
- <a id="s-11883d4162"></a>`additionalProperties`: `false`
- <a id="s-33ed83e1ef"></a>`required`: `["source_id","claim_id","source_event_id","file_count","bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5e79ba7532"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-7ba02835e6"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-17f74010ac"></a>`file_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-9cbd20138b"></a>`source_event_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-020145bb63"></a>`source_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |

##### <a id="s-e256f516e3"></a>definition `FtpLifecycleEvent`

- <a id="s-cc5506a14d"></a>`discriminator`: `{"mapping":{"io.riverhog.ftp_spool.claim.attempt_failed":"#/$defs/AttemptFailedEvent","io.riverhog.ftp_spool.claim.custody_ready":"#/$defs/CustodyReadyEvent","io.riverhog.ftp_spool.claim.published":"#/$defs/ClaimPublishedEvent","io.riverhog.ftp_spool.claim.registered":"#/$defs/ClaimRegisteredEvent"},"propertyName":"type"}`

###### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-27b474af92"></a>1 | [ClaimRegisteredEvent](#s-870657c4ba) |
| <a id="s-17930915f3"></a>2 | [CustodyReadyEvent](#s-2529d76c75) |
| <a id="s-54aa524374"></a>3 | [AttemptFailedEvent](#s-f2414a5286) |
| <a id="s-a34ce78e3f"></a>4 | [ClaimPublishedEvent](#s-ef52d4b674) |

## Maintained corroboration

### Related interface records

- [require_progress_after](a-riverhog-ftp-spool-client-ftpeventpage-require-progress-after.md)

## Governing policies

- <a id="pa-481a5c8441"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.FtpEventPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 045d0639453d8f3176d4379f5da508490c14f63b38868d05250c6cf017d87578 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "AttemptFailedEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/AttemptFailedPayload"
            },
            "subject": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.ftp_spool.claim.attempt_failed",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "subject",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "AttemptFailedPayload": {
          "additionalProperties": false,
          "properties": {
            "claim_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "error_type": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "source_event_id": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            },
            "source_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "source_id",
            "claim_id",
            "source_event_id",
            "error_type"
          ],
          "type": "object"
        },
        "ClaimPublishedEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/ClaimPublishedPayload"
            },
            "subject": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.ftp_spool.claim.published",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "subject",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "ClaimPublishedPayload": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "claim_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "collection_id": {
              "pattern": "^[1-9][0-9]*$",
              "type": "string"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "source_event_id": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            },
            "source_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "source_id",
            "claim_id",
            "source_event_id",
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "type": "object"
        },
        "ClaimRegisteredEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/ClaimRegisteredPayload"
            },
            "subject": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.ftp_spool.claim.registered",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "subject",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "ClaimRegisteredPayload": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "claim_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "file_count": {
              "minimum": 1,
              "type": "integer"
            },
            "source_event_id": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            },
            "source_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "source_id",
            "claim_id",
            "source_event_id",
            "file_count",
            "bytes"
          ],
          "type": "object"
        },
        "CustodyReadyEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/CustodyReadyPayload"
            },
            "subject": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.ftp_spool.claim.custody_ready",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "subject",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "CustodyReadyPayload": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "claim_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "file_count": {
              "minimum": 1,
              "type": "integer"
            },
            "source_event_id": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            },
            "source_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "source_id",
            "claim_id",
            "source_event_id",
            "file_count",
            "bytes"
          ],
          "type": "object"
        },
        "FtpLifecycleEvent": {
          "discriminator": {
            "mapping": {
              "io.riverhog.ftp_spool.claim.attempt_failed": "#/$defs/AttemptFailedEvent",
              "io.riverhog.ftp_spool.claim.custody_ready": "#/$defs/CustodyReadyEvent",
              "io.riverhog.ftp_spool.claim.published": "#/$defs/ClaimPublishedEvent",
              "io.riverhog.ftp_spool.claim.registered": "#/$defs/ClaimRegisteredEvent"
            },
            "propertyName": "type"
          },
          "oneOf": [
            {
              "$ref": "#/$defs/ClaimRegisteredEvent"
            },
            {
              "$ref": "#/$defs/CustodyReadyEvent"
            },
            {
              "$ref": "#/$defs/AttemptFailedEvent"
            },
            {
              "$ref": "#/$defs/ClaimPublishedEvent"
            }
          ]
        }
      },
      "additionalProperties": false,
      "properties": {
        "events": {
          "items": {
            "$ref": "#/$defs/FtpLifecycleEvent"
          },
          "maxItems": 100,
          "type": "array"
        },
        "has_more": {
          "type": "boolean"
        },
        "next_cursor": {
          "maxLength": 220,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "events",
        "next_cursor",
        "has_more"
      ],
      "type": "object"
    },
    "signature": "'(*, events: Annotated[list[FtpLifecycleEvent], MaxLen(max_length=100)], next_cursor: Annotated[str, MinLen(min_length=1), MaxLen(max_length=220)], has_more: bool) -> None'"
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "FtpEventPage",
  "unit": "export"
}
```

</details>
