# a-riverhog-b2-store:configuration:b2-store-document configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:a-riverhog-b2-store:a-riverhog-b2-store-configuration-b2-stor-9d2b784c74:fa4386a7ac -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-7bd03a5b28"></a>

- <a id="s-d369a0047d"></a>`type`: `"object"`
- <a id="s-dd330db502"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/a-riverhog-b2-store.schema.json"`
- <a id="s-093bdaef99"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-9c3d8d9f75"></a>`additionalProperties`: `false`
- <a id="s-d735cc1071"></a>`required`: `["bucket","region","endpoint_url","token_file","access_key_id_file","secret_access_key_file"]`
- <a id="s-812ed33975"></a>`title`: `"B2StoreDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fbcea78ad6"></a>`access_key_id_file` | yes | type="string"; format="path"; title="Access Key Id File" |  |
| <a id="s-0b889278dc"></a>`bucket` | yes | type="string"; minLength=1; title="Bucket" |  |
| <a id="s-5027b32233"></a>`connect_timeout_seconds` | no | type="number"; default=10; exclusiveMinimum=0; title="Connect Timeout Seconds" |  |
| <a id="s-68dfab4060"></a>`endpoint_url` | yes | type="string"; minLength=1; title="Endpoint Url" |  |
| <a id="s-08374cecbc"></a>`force_path_style` | no | type="boolean"; default=false; title="Force Path Style" |  |
| <a id="s-fffe0c2835"></a>`max_attempts` | no | type="integer"; minimum=1; maximum=100; default=8; title="Max Attempts" |  |
| <a id="s-b29c993574"></a>`max_pool_connections` | no | type="integer"; minimum=1; maximum=4096; default=32; title="Max Pool Connections" |  |
| <a id="s-3d06fd0a42"></a>`read_chunk_bytes` | no | type="integer"; minimum=65536; default=8388608; title="Read Chunk Bytes" |  |
| <a id="s-fa79d726d2"></a>`read_timeout_seconds` | no | type="number"; default=300; exclusiveMinimum=0; title="Read Timeout Seconds" |  |
| <a id="s-c9af555cee"></a>`region` | yes | type="string"; minLength=1; title="Region" |  |
| <a id="s-e4c4332377"></a>`retry_mode` | no | type="string"; enum=["standard","adaptive"]; default="standard"; title="Retry Mode" |  |
| <a id="s-5202ff39fc"></a>`root_prefix` | no | type="string"; default=""; title="Root Prefix" |  |
| <a id="s-a9044f8d3c"></a>`secret_access_key_file` | yes | type="string"; format="path"; title="Secret Access Key File" |  |
| <a id="s-86898d92c6"></a>`session_token_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Session Token File" |  |
| <a id="s-1c74e9703d"></a>`tcp_keepalive` | no | type="boolean"; default=true; title="Tcp Keepalive" |  |
| <a id="s-7a12c2c299"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field max_attempts](#s-fffe0c2835) | `value · schema-value · contract_max` | maximum=100 |
| [field max_pool_connections](#s-b29c993574) | `value · schema-value · contract_max` | maximum=4096 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c8f9a7966b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-dd9237292e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:a-riverhog-b2-store:configuration:b2-store-document](../../../evidence/sources/authorities.md#src-2e569d1ae6) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::B2StoreDocument](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/a-riverhog-b2-store:configuration:b2-store-document`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 63d8546902cc34e3cd4b70f5971edb8994dadb20ad455073298df509774f682d -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/config/a-riverhog-b2-store.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "access_key_id_file": {
      "format": "path",
      "title": "Access Key Id File",
      "type": "string"
    },
    "bucket": {
      "minLength": 1,
      "title": "Bucket",
      "type": "string"
    },
    "connect_timeout_seconds": {
      "default": 10,
      "exclusiveMinimum": 0,
      "title": "Connect Timeout Seconds",
      "type": "number"
    },
    "endpoint_url": {
      "minLength": 1,
      "title": "Endpoint Url",
      "type": "string"
    },
    "force_path_style": {
      "default": false,
      "title": "Force Path Style",
      "type": "boolean"
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
    "endpoint_url",
    "token_file",
    "access_key_id_file",
    "secret_access_key_file"
  ],
  "title": "B2StoreDocument",
  "type": "object"
}
```

</details>
