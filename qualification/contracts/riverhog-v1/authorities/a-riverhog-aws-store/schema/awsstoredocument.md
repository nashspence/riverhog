# AwsStoreDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:a-riverhog-aws-store:awsstoredocument:c719626615 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-7373ff9eda"></a>

- <a id="s-2d09e080af"></a>`type`: `"object"`
- <a id="s-4aee104131"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/a-riverhog-aws-store.schema.json"`
- <a id="s-f4686ec267"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-c23ae7bde3"></a>`additionalProperties`: `false`
- <a id="s-e73bf4407d"></a>`required`: `["bucket","region","token_file","access_key_id_file","secret_access_key_file"]`
- <a id="s-b0edcfc4a1"></a>`title`: `"AwsStoreDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aec2bde450"></a>`access_key_id_file` | yes | type="string"; format="path"; title="Access Key Id File" |  |
| <a id="s-88f265527f"></a>`archive_storage_class` | no | type="string"; default="DEEP_ARCHIVE"; minLength=1; title="Archive Storage Class" |  |
| <a id="s-7387085907"></a>`bucket` | yes | type="string"; minLength=1; title="Bucket" |  |
| <a id="s-02db1d3cd1"></a>`cloudfront` | no | anyOf=[([AwsCloudFrontDocument](#s-d9f604b0bb)); (type="null")]; default=null |  |
| <a id="s-34146f81f3"></a>`connect_timeout_seconds` | no | type="number"; default=10; exclusiveMinimum=0; title="Connect Timeout Seconds" |  |
| <a id="s-4b06829c70"></a>`endpoint_url` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Endpoint Url" |  |
| <a id="s-bf3bc15eb4"></a>`force_path_style` | no | type="boolean"; default=false; title="Force Path Style" |  |
| <a id="s-dd4e9e6ca5"></a>`immediate_storage_class` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Immediate Storage Class" |  |
| <a id="s-b0f0c8b342"></a>`max_attempts` | no | type="integer"; minimum=1; maximum=100; default=8; title="Max Attempts" |  |
| <a id="s-f13fffd421"></a>`max_pool_connections` | no | type="integer"; minimum=1; maximum=4096; default=32; title="Max Pool Connections" |  |
| <a id="s-89db337f57"></a>`read_chunk_bytes` | no | type="integer"; minimum=65536; default=8388608; title="Read Chunk Bytes" |  |
| <a id="s-95ac8595cd"></a>`read_mode` | no | type="string"; enum=["immediate","restore_required"]; default="restore_required"; title="Read Mode" |  |
| <a id="s-d0a5c0d16d"></a>`read_timeout_seconds` | no | type="number"; default=300; exclusiveMinimum=0; title="Read Timeout Seconds" |  |
| <a id="s-f2de849f98"></a>`region` | yes | type="string"; minLength=1; title="Region" |  |
| <a id="s-24fa835685"></a>`restore_days` | no | type="integer"; minimum=1; default=3; title="Restore Days" |  |
| <a id="s-6f71549cd6"></a>`restore_tier` | no | type="string"; enum=["Bulk","Standard","Expedited"]; default="Bulk"; title="Restore Tier" |  |
| <a id="s-b94d7a9c92"></a>`retry_mode` | no | type="string"; enum=["standard","adaptive"]; default="standard"; title="Retry Mode" |  |
| <a id="s-ddbe287be9"></a>`root_prefix` | no | type="string"; default=""; title="Root Prefix" |  |
| <a id="s-54f83c9a39"></a>`secret_access_key_file` | yes | type="string"; format="path"; title="Secret Access Key File" |  |
| <a id="s-550504d031"></a>`session_token_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Session Token File" |  |
| <a id="s-544d8298a5"></a>`tcp_keepalive` | no | type="boolean"; default=true; title="Tcp Keepalive" |  |
| <a id="s-bcebdffd6b"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### Definitions

- [AwsCloudFrontDocument](#s-d9f604b0bb)

### <a id="s-d9f604b0bb"></a>definition `AwsCloudFrontDocument`

- <a id="s-9b2b33234e"></a>`type`: `"object"`
- <a id="s-304b00b72d"></a>`additionalProperties`: `false`
- <a id="s-e2bb9ebd8b"></a>`required`: `["base_url","public_key_id","private_key_path"]`
- <a id="s-a66f1f497f"></a>`title`: `"AwsCloudFrontDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8ba5bf8274"></a>`base_url` | yes | type="string"; minLength=1; title="Base Url" |  |
| <a id="s-c522c39907"></a>`private_key_path` | yes | type="string"; format="path"; title="Private Key Path" |  |
| <a id="s-b631576400"></a>`public_key_id` | yes | type="string"; minLength=1; title="Public Key Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field max_attempts](#s-b0f0c8b342) | `value · schema-value · contract_max` | maximum=100 |
| [field max_pool_connections](#s-f13fffd421) | `value · schema-value · contract_max` | maximum=4096 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-1ad451b888"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-26348edcd2"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/config/a-riverhog-aws-store.schema.json](../../../evidence/sources/authorities.md#src-4e7eb1f588) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/config.schema.json](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/config.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1config~1a-riverhog-aws-store.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa7e1654b1452b8ee068942187cbff62f0b1f143d151f10754974955d162fda5 -->

```json
{
  "$defs": {
    "AwsCloudFrontDocument": {
      "additionalProperties": false,
      "properties": {
        "base_url": {
          "minLength": 1,
          "title": "Base Url",
          "type": "string"
        },
        "private_key_path": {
          "format": "path",
          "title": "Private Key Path",
          "type": "string"
        },
        "public_key_id": {
          "minLength": 1,
          "title": "Public Key Id",
          "type": "string"
        }
      },
      "required": [
        "base_url",
        "public_key_id",
        "private_key_path"
      ],
      "title": "AwsCloudFrontDocument",
      "type": "object"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/config/a-riverhog-aws-store.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "access_key_id_file": {
      "format": "path",
      "title": "Access Key Id File",
      "type": "string"
    },
    "archive_storage_class": {
      "default": "DEEP_ARCHIVE",
      "minLength": 1,
      "title": "Archive Storage Class",
      "type": "string"
    },
    "bucket": {
      "minLength": 1,
      "title": "Bucket",
      "type": "string"
    },
    "cloudfront": {
      "anyOf": [
        {
          "$ref": "#/$defs/AwsCloudFrontDocument"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "connect_timeout_seconds": {
      "default": 10,
      "exclusiveMinimum": 0,
      "title": "Connect Timeout Seconds",
      "type": "number"
    },
    "endpoint_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Endpoint Url"
    },
    "force_path_style": {
      "default": false,
      "title": "Force Path Style",
      "type": "boolean"
    },
    "immediate_storage_class": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Immediate Storage Class"
    },
    "max_attempts": {
      "default": 8,
      "maximum": 100,
      "minimum": 1,
      "title": "Max Attempts",
      "type": "integer"
    },
    "max_pool_connections": {
      "default": 32,
      "maximum": 4096,
      "minimum": 1,
      "title": "Max Pool Connections",
      "type": "integer"
    },
    "read_chunk_bytes": {
      "default": 8388608,
      "minimum": 65536,
      "title": "Read Chunk Bytes",
      "type": "integer"
    },
    "read_mode": {
      "default": "restore_required",
      "enum": [
        "immediate",
        "restore_required"
      ],
      "title": "Read Mode",
      "type": "string"
    },
    "read_timeout_seconds": {
      "default": 300,
      "exclusiveMinimum": 0,
      "title": "Read Timeout Seconds",
      "type": "number"
    },
    "region": {
      "minLength": 1,
      "title": "Region",
      "type": "string"
    },
    "restore_days": {
      "default": 3,
      "minimum": 1,
      "title": "Restore Days",
      "type": "integer"
    },
    "restore_tier": {
      "default": "Bulk",
      "enum": [
        "Bulk",
        "Standard",
        "Expedited"
      ],
      "title": "Restore Tier",
      "type": "string"
    },
    "retry_mode": {
      "default": "standard",
      "enum": [
        "standard",
        "adaptive"
      ],
      "title": "Retry Mode",
      "type": "string"
    },
    "root_prefix": {
      "default": "",
      "title": "Root Prefix",
      "type": "string"
    },
    "secret_access_key_file": {
      "format": "path",
      "title": "Secret Access Key File",
      "type": "string"
    },
    "session_token_file": {
      "anyOf": [
        {
          "format": "path",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Session Token File"
    },
    "tcp_keepalive": {
      "default": true,
      "title": "Tcp Keepalive",
      "type": "boolean"
    },
    "token_file": {
      "format": "path",
      "title": "Token File",
      "type": "string"
    }
  },
  "required": [
    "bucket",
    "region",
    "token_file",
    "access_key_id_file",
    "secret_access_key_file"
  ],
  "title": "AwsStoreDocument",
  "type": "object"
}
```

</details>
