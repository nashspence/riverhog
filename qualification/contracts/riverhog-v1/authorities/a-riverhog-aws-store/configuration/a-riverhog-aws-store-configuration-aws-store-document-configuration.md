# a-riverhog-aws-store:configuration:aws-store-document configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:a-riverhog-aws-store:a-riverhog-aws-store-configuration-aws-st-000ecbee6c:5eaa3356a6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-44709ca608"></a>

- <a id="s-cdf1173846"></a>`type`: `"object"`
- <a id="s-ff25a6a0b4"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/a-riverhog-aws-store.schema.json"`
- <a id="s-5200cb5ad7"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-80efb0a417"></a>`additionalProperties`: `false`
- <a id="s-e79885464b"></a>`required`: `["bucket","region","token_file","access_key_id_file","secret_access_key_file"]`
- <a id="s-a09b893b69"></a>`title`: `"AwsStoreDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6192927b12"></a>`access_key_id_file` | yes | type="string"; format="path"; title="Access Key Id File" |  |
| <a id="s-6ed64f147f"></a>`archive_storage_class` | no | type="string"; default="DEEP_ARCHIVE"; minLength=1; title="Archive Storage Class" |  |
| <a id="s-fd80ef1935"></a>`bucket` | yes | type="string"; minLength=1; title="Bucket" |  |
| <a id="s-705a7b5258"></a>`cloudfront` | no | anyOf=[([AwsCloudFrontDocument](#s-1014d7a22f)); (type="null")]; default=null |  |
| <a id="s-514801b189"></a>`connect_timeout_seconds` | no | type="number"; default=10; exclusiveMinimum=0; title="Connect Timeout Seconds" |  |
| <a id="s-6b96322361"></a>`endpoint_url` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Endpoint Url" |  |
| <a id="s-aa71c64e42"></a>`force_path_style` | no | type="boolean"; default=false; title="Force Path Style" |  |
| <a id="s-c1c3325ff9"></a>`immediate_storage_class` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Immediate Storage Class" |  |
| <a id="s-4dc82bb32c"></a>`max_attempts` | no | type="integer"; minimum=1; maximum=100; default=8; title="Max Attempts" |  |
| <a id="s-89fc1414eb"></a>`max_pool_connections` | no | type="integer"; minimum=1; maximum=4096; default=32; title="Max Pool Connections" |  |
| <a id="s-15bfe7b6aa"></a>`read_chunk_bytes` | no | type="integer"; minimum=65536; default=8388608; title="Read Chunk Bytes" |  |
| <a id="s-31997f6268"></a>`read_mode` | no | type="string"; enum=["immediate","restore_required"]; default="restore_required"; title="Read Mode" |  |
| <a id="s-64e2280911"></a>`read_timeout_seconds` | no | type="number"; default=300; exclusiveMinimum=0; title="Read Timeout Seconds" |  |
| <a id="s-3d95cb05b7"></a>`region` | yes | type="string"; minLength=1; title="Region" |  |
| <a id="s-743e937b3f"></a>`restore_days` | no | type="integer"; minimum=1; default=3; title="Restore Days" |  |
| <a id="s-0111011414"></a>`restore_tier` | no | type="string"; enum=["Bulk","Standard","Expedited"]; default="Bulk"; title="Restore Tier" |  |
| <a id="s-8754ea1586"></a>`retry_mode` | no | type="string"; enum=["standard","adaptive"]; default="standard"; title="Retry Mode" |  |
| <a id="s-c5a8aaa187"></a>`root_prefix` | no | type="string"; default=""; title="Root Prefix" |  |
| <a id="s-243d97f2c9"></a>`secret_access_key_file` | yes | type="string"; format="path"; title="Secret Access Key File" |  |
| <a id="s-6b5589c817"></a>`session_token_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Session Token File" |  |
| <a id="s-e52b783763"></a>`tcp_keepalive` | no | type="boolean"; default=true; title="Tcp Keepalive" |  |
| <a id="s-7eb5f8ad0b"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### Definitions

- [AwsCloudFrontDocument](#s-1014d7a22f)

### <a id="s-1014d7a22f"></a>definition `AwsCloudFrontDocument`

- <a id="s-cd1cd81311"></a>`type`: `"object"`
- <a id="s-f689940363"></a>`additionalProperties`: `false`
- <a id="s-89184fc3ee"></a>`required`: `["base_url","public_key_id","private_key_path"]`
- <a id="s-0d7d0dbd61"></a>`title`: `"AwsCloudFrontDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-233279fdec"></a>`base_url` | yes | type="string"; minLength=1; title="Base Url" |  |
| <a id="s-400e6df157"></a>`private_key_path` | yes | type="string"; format="path"; title="Private Key Path" |  |
| <a id="s-09c6ee6da1"></a>`public_key_id` | yes | type="string"; minLength=1; title="Public Key Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field max_attempts](#s-4dc82bb32c) | `value · schema-value · contract_max` | maximum=100 |
| [field max_pool_connections](#s-89fc1414eb) | `value · schema-value · contract_max` | maximum=4096 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6cefa387c9"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-88d6af0723"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:a-riverhog-aws-store:configuration:aws-store-document](../../../evidence/sources/authorities.md#src-6f341e5f70) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::AwsStoreDocument](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/a-riverhog-aws-store:configuration:aws-store-document`

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
