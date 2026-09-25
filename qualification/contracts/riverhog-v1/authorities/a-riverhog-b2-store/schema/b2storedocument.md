# B2StoreDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:a-riverhog-b2-store:b2storedocument:c1c8e7d655 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-7257aca593"></a>

- <a id="s-ae1e9a95e1"></a>`type`: `"object"`
- <a id="s-7e4c935715"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/a-riverhog-b2-store.schema.json"`
- <a id="s-7a1090212d"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-b6a80ccf13"></a>`additionalProperties`: `false`
- <a id="s-c9f0c5df35"></a>`required`: `["bucket","region","endpoint_url","token_file","access_key_id_file","secret_access_key_file"]`
- <a id="s-9893b72695"></a>`title`: `"B2StoreDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b2c0d37e80"></a>`access_key_id_file` | yes | type="string"; format="path"; title="Access Key Id File" |  |
| <a id="s-48cd1f2e90"></a>`bucket` | yes | type="string"; minLength=1; title="Bucket" |  |
| <a id="s-57281b53b1"></a>`connect_timeout_seconds` | no | type="number"; default=10; exclusiveMinimum=0; title="Connect Timeout Seconds" |  |
| <a id="s-ce5e66aa47"></a>`endpoint_url` | yes | type="string"; minLength=1; title="Endpoint Url" |  |
| <a id="s-c7348f6dba"></a>`force_path_style` | no | type="boolean"; default=false; title="Force Path Style" |  |
| <a id="s-0ad50ba6ed"></a>`max_attempts` | no | type="integer"; minimum=1; maximum=100; default=8; title="Max Attempts" |  |
| <a id="s-bba22f7f40"></a>`max_pool_connections` | no | type="integer"; minimum=1; maximum=4096; default=32; title="Max Pool Connections" |  |
| <a id="s-d46a449a6f"></a>`read_chunk_bytes` | no | type="integer"; minimum=65536; default=8388608; title="Read Chunk Bytes" |  |
| <a id="s-2631fb5172"></a>`read_timeout_seconds` | no | type="number"; default=300; exclusiveMinimum=0; title="Read Timeout Seconds" |  |
| <a id="s-8c7d7803f2"></a>`region` | yes | type="string"; minLength=1; title="Region" |  |
| <a id="s-f50b7be41f"></a>`retry_mode` | no | type="string"; enum=["standard","adaptive"]; default="standard"; title="Retry Mode" |  |
| <a id="s-ba65690e4c"></a>`root_prefix` | no | type="string"; default=""; title="Root Prefix" |  |
| <a id="s-2369084bd1"></a>`secret_access_key_file` | yes | type="string"; format="path"; title="Secret Access Key File" |  |
| <a id="s-a8457c34d8"></a>`session_token_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Session Token File" |  |
| <a id="s-e5a420fb6f"></a>`tcp_keepalive` | no | type="boolean"; default=true; title="Tcp Keepalive" |  |
| <a id="s-a1479b7db8"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field max_attempts](#s-0ad50ba6ed) | `value · schema-value · contract_max` | maximum=100 |
| [field max_pool_connections](#s-bba22f7f40) | `value · schema-value · contract_max` | maximum=4096 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-85e4642e7b"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-dc017c60c0"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/config/a-riverhog-b2-store.schema.json](../../../evidence/sources/authorities.md#src-433da1ee08) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/config.schema.json](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/config.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1config~1a-riverhog-b2-store.schema.json`

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
