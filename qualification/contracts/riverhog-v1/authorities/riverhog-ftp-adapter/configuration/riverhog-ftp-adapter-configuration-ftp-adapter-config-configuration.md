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

- <a id="s-fe9ddcb15a"></a>`type`: `"object"`
- <a id="s-5c64d0ade2"></a>`additionalProperties`: `false`
- <a id="s-7a6d4adcb1"></a>`required`: `["host_id","riverhog_base_url","riverhog_token","api_token","sources"]`
- <a id="s-16e59a37de"></a>`title`: `"FtpAdapterConfig"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f911dd1f94"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-dbf5ea4ac5"></a>`api_token` | yes | type="string"; maxLength=4096; minLength=1; title="Api Token" |  |
| <a id="s-7c7fe84c8b"></a>`claim_attempt_budget` | no | type="integer"; minimum=2; default=8; title="Claim Attempt Budget" |  |
| <a id="s-684f7aa9c4"></a>`completion_failure_attempt_budget` | no | type="integer"; minimum=1; default=8; title="Completion Failure Attempt Budget" |  |
| <a id="s-c151434722"></a>`completion_failure_capacity` | no | type="integer"; minimum=1; default=128; title="Completion Failure Capacity" |  |
| <a id="s-6c890fc5d5"></a>`discovery_entry_budget` | no | type="integer"; minimum=1; default=4096; title="Discovery Entry Budget" |  |
| <a id="s-72ad5db1e6"></a>`host_id` | yes | type="string"; maxLength=255; minLength=1; title="Host Id" |  |
| <a id="s-7c5fa78bfc"></a>`pending_claim_capacity` | no | type="integer"; minimum=1; default=128; title="Pending Claim Capacity" |  |
| <a id="s-82aa3eb24d"></a>`poll_seconds` | no | type="number"; minimum=0.1; maximum=3600; default=5; title="Poll Seconds" |  |
| <a id="s-2a409954ca"></a>`provenance_observer` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Provenance Observer" |  |
| <a id="s-22a68b6f37"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1; title="Riverhog Base Url" |  |
| <a id="s-132c5582fa"></a>`riverhog_token` | yes | type="string"; maxLength=4096; minLength=1; title="Riverhog Token" |  |
| <a id="s-c96e3a57ab"></a>`sources` | yes | type="array"; items=([SourceConfig](#s-b4cbe4146f)); minItems=1; title="Sources" |  |

### Definitions

- [CollectionDescription](#s-de0b3f6676)
- [CollectionTag](#s-031bfd2553)
- [SourceConfig](#s-b4cbe4146f)

### <a id="s-de0b3f6676"></a>definition `CollectionDescription`

- <a id="s-4692000325"></a>`type`: `"string"`
- <a id="s-5b25e1f476"></a>`maxLength`: `32768`
- <a id="s-f7061fa30a"></a>`minLength`: `1`
- <a id="s-01f4d2aa7c"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-144d66539f"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-393294ed07"></a>`x-unicode-normalization`: `"NFC"`

### <a id="s-031bfd2553"></a>definition `CollectionTag`

- <a id="s-dce56ffd8e"></a>`type`: `"string"`
- <a id="s-4e5db1c1f6"></a>`maxLength`: `65536`
- <a id="s-4c9a7f815f"></a>`minLength`: `1`
- <a id="s-06092b1f00"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-cc2b9cfcbe"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-192cff69a4"></a>`x-unicode-normalization`: `"NFC"`

### <a id="s-b4cbe4146f"></a>definition `SourceConfig`

- <a id="s-f0f381abae"></a>`type`: `"object"`
- <a id="s-0862e6a3e3"></a>`additionalProperties`: `false`
- <a id="s-36753f0b80"></a>`description`: `"One deployment-owned, content-opaque intake source."`
- <a id="s-f7da26f3bb"></a>`required`: `["id","root","ingest_source"]`
- <a id="s-1275c30dd4"></a>`title`: `"SourceConfig"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3246c539d9"></a>`archive_store` | no | anyOf=[(type="string"; maxLength=160; minLength=1); (type="null")]; default=null; title="Archive Store" |  |
| <a id="s-9711d6753d"></a>`close_mode` | no | type="string"; enum=["stable","explicit-flush"]; default="stable"; title="Close Mode" |  |
| <a id="s-276db63837"></a>`description` | no | anyOf=[([CollectionDescription](#s-de0b3f6676)); (type="null")]; default=null |  |
| <a id="s-8f393e4b41"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Id" |  |
| <a id="s-d364f97555"></a>`ingest_source` | yes | type="string"; maxLength=512; minLength=1; title="Ingest Source" |  |
| <a id="s-16b095e18a"></a>`max_bytes` | no | type="integer"; minimum=1; default=107374182400; title="Max Bytes" |  |
| <a id="s-487e91e300"></a>`max_files` | no | type="integer"; minimum=1; default=1000; title="Max Files" |  |
| <a id="s-78d458ca6a"></a>`provenance` | no | type="string"; enum=["capture","omit"]; default="capture"; title="Provenance" |  |
| <a id="s-a05abaefd3"></a>`provenance_omission_reason` | no | anyOf=[(type="string"; maxLength=1000); (type="null")]; default=null; title="Provenance Omission Reason" |  |
| <a id="s-042095c12f"></a>`root` | yes | type="string"; format="path"; title="Root" |  |
| <a id="s-779c3d2f99"></a>`tags` | no | type="array"; default=[]; items=([CollectionTag](#s-031bfd2553)); title="Tags" |  |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-ftp-adapter:configuration:ftp-adapter-config"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition SourceConfig · field tags](#s-779c3d2f99) | `cardinality · items · operational_policy` | shared above |
| [field sources](#s-c96e3a57ab) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition CollectionDescription](#s-de0b3f6676) | `encoded-size · bytes · contract_max` | maximum=32768; reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionDescription](#s-de0b3f6676) | `length · characters · contract_max` | maximum=32768; minimum=1; reason="schema-maximum" |
| [definition CollectionTag](#s-031bfd2553) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionTag](#s-031bfd2553) | `length · characters · contract_max` | maximum=65536; minimum=1; reason="schema-maximum" |
| <a id="s-58715b7340"></a>[definition SourceConfig · field archive_store · string value](#s-3246c539d9) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [definition SourceConfig · field ingest_source](#s-d364f97555) | `length · characters · contract_max` | maximum=512; minimum=1; reason="schema-maximum" |
| <a id="s-6e40570cee"></a>[definition SourceConfig · field provenance_omission_reason · string value](#s-a05abaefd3) | `length · characters · contract_max` | maximum=1000; reason="schema-maximum" |
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

- [configuration:riverhog-ftp-adapter:configuration:ftp-adapter-config](../../../evidence/sources.md#src-cf8c44826f) — [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/config.py::FtpAdapterConfig](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/riverhog-ftp-adapter:configuration:ftp-adapter-config`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
