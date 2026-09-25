# RiverhogDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-server:riverhogdocument:9b36a5e595 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-0dc5d3bb84"></a>

- <a id="s-1f587e4b42"></a>`type`: `"object"`
- <a id="s-6d231631af"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/riverhog-server.schema.json"`
- <a id="s-642a964530"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-294257b759"></a>`additionalProperties`: `false`
- <a id="s-812e91dd9e"></a>`required`: `["database_url_file","bootstrap_token_file","browse_token_signing_key_file","archive_passphrase_files","archive_active_passphrase_id","archive_write_store","archive_stores"]`
- <a id="s-573af8553c"></a>`title`: `"RiverhogDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-de822641af"></a>`archive_active_passphrase_id` | yes | type="string"; minLength=1; title="Archive Active Passphrase Id" |  |
| <a id="s-ea597240c6"></a>`archive_passphrase_files` | yes | type="object"; additionalProperties=(type="string"; format="path"); minProperties=1; title="Archive Passphrase Files" |  |
| <a id="s-002c9a9bea"></a>`archive_read_order` | no | type="array"; items=(type="string"); title="Archive Read Order" |  |
| <a id="s-5f3ec0388f"></a>`archive_scrypt_work_factor` | no | type="integer"; minimum=1; maximum=22; default=18; title="Archive Scrypt Work Factor" |  |
| <a id="s-854c5e0c07"></a>`archive_stores` | yes | type="object"; additionalProperties=([ArchiveStoreDocument](#s-c7477255a2)); minProperties=1; title="Archive Stores" |  |
| <a id="s-9047a5a3b4"></a>`archive_upload_sweep_interval` | no | type="string"; default="30s"; title="Archive Upload Sweep Interval" |  |
| <a id="s-fc64d6f3c9"></a>`archive_write_store` | yes | type="string"; minLength=1; title="Archive Write Store" |  |
| <a id="s-91872ef695"></a>`bootstrap_token_file` | yes | type="string"; format="path"; title="Bootstrap Token File" |  |
| <a id="s-c546958afb"></a>`browse_token_lifetime` | no | type="string"; default="24h"; title="Browse Token Lifetime" |  |
| <a id="s-8f97b41c17"></a>`browse_token_signing_key_file` | yes | type="string"; format="path"; title="Browse Token Signing Key File" |  |
| <a id="s-3761e04ea1"></a>`catalog_sync_bootstrap_lifetime` | no | type="string"; default="7d"; title="Catalog Sync Bootstrap Lifetime" |  |
| <a id="s-9fca39cb22"></a>`catalog_sync_cursor_lifetime` | no | type="string"; default="24h"; title="Catalog Sync Cursor Lifetime" |  |
| <a id="s-bdcc2974ef"></a>`catalog_sync_history_reap_batch_size` | no | type="integer"; minimum=1; default=100; title="Catalog Sync History Reap Batch Size" |  |
| <a id="s-7f99f2962b"></a>`catalog_sync_history_retention` | no | type="string"; default="30d"; title="Catalog Sync History Retention" |  |
| <a id="s-f4b8c66866"></a>`catalog_sync_page_size_max` | no | type="integer"; minimum=1; maximum=100; default=100; title="Catalog Sync Page Size Max" |  |
| <a id="s-1792421431"></a>`collection_upload_custody_lease` | no | type="string"; default="1h"; title="Collection Upload Custody Lease" |  |
| <a id="s-f577468457"></a>`database_url_file` | yes | type="string"; format="path"; title="Database Url File" |  |
| <a id="s-d1fb1af115"></a>`event_context_reap_batch_size` | no | type="integer"; minimum=1; default=100; title="Event Context Reap Batch Size" |  |
| <a id="s-18fe99e52b"></a>`event_context_retention` | no | type="string"; default="30d"; title="Event Context Retention" |  |
| <a id="s-2a5c212066"></a>`log_level` | no | type="string"; default="INFO"; title="Log Level" |  |
| <a id="s-92e2d66278"></a>`public_base_url` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Public Base Url" |  |
| <a id="s-60d06e3c73"></a>`range_policy` | no | [RangePolicyDocument](#s-a1fac1c36d) |  |
| <a id="s-4093261341"></a>`range_policy_by_store` | no | type="object"; additionalProperties=([RangePolicyDocument](#s-a1fac1c36d)); title="Range Policy By Store" |  |
| <a id="s-545592c074"></a>`retrieval_cache_new_archive_enabled` | no | type="boolean"; default=true; title="Retrieval Cache New Archive Enabled" |  |
| <a id="s-0ca703bb2b"></a>`retrieval_cache_new_archive_lease` | no | type="string"; default="72h"; title="Retrieval Cache New Archive Lease" |  |
| <a id="s-edb16f7a86"></a>`retrieval_cache_stores` | no | type="object"; additionalProperties=([CacheStoreDocument](#s-7bd5bf1374)); title="Retrieval Cache Stores" |  |
| <a id="s-af6f2467e8"></a>`retrieval_cache_sweep_interval` | no | type="string"; default="5m"; title="Retrieval Cache Sweep Interval" |  |
| <a id="s-bfcb27c288"></a>`retrieval_cache_write_segment_bytes` | no | type="string"; default="67108864"; title="Retrieval Cache Write Segment Bytes" |  |
| <a id="s-ecfdf9f2e3"></a>`retrieval_default_lease` | no | type="string"; default="24h"; title="Retrieval Default Lease" |  |
| <a id="s-a1c4f0a6fd"></a>`retrieval_estimated_latency` | no | type="string"; default="48h"; title="Retrieval Estimated Latency" |  |
| <a id="s-47f467fcff"></a>`retrieval_max_lease` | no | type="string"; default="7d"; title="Retrieval Max Lease" |  |
| <a id="s-697a1da69d"></a>`retrieval_pending_timeout` | no | type="string"; default="72h"; title="Retrieval Pending Timeout" |  |
| <a id="s-5d99ecdb91"></a>`retrieval_restore_poll_interval` | no | type="string"; default="5m"; title="Retrieval Restore Poll Interval" |  |
| <a id="s-bac5fc7b58"></a>`throughput` | no | [ThroughputDocument](#s-d58ac68fd5) |  |
| <a id="s-25981e487e"></a>`volume_policy` | no | [VolumePolicyDocument](#s-9a7157d572) |  |

### Definitions

- [ArchiveStoreDocument](#s-c7477255a2)
- [CacheStoreDocument](#s-7bd5bf1374)
- [RangePolicyDocument](#s-a1fac1c36d)
- [ThroughputDocument](#s-d58ac68fd5)
- [VolumePolicyDocument](#s-9a7157d572)

### <a id="s-c7477255a2"></a>definition `ArchiveStoreDocument`

- <a id="s-f34d4c3899"></a>`type`: `"object"`
- <a id="s-2d9469e83d"></a>`additionalProperties`: `false`
- <a id="s-d1557be5fd"></a>`required`: `["base_url","token_file"]`
- <a id="s-2a6e635c0b"></a>`title`: `"ArchiveStoreDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-19793c0594"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-e776113acb"></a>`base_url` | yes | type="string"; minLength=1; title="Base Url" |  |
| <a id="s-07d60c40b9"></a>`download_safety_buffer_bytes` | no | type="string"; default="0B"; title="Download Safety Buffer Bytes" |  |
| <a id="s-f0ced3b92f"></a>`maximum_connections` | no | type="integer"; minimum=1; default=32; title="Maximum Connections" |  |
| <a id="s-21304f98fe"></a>`monthly_download_allowance_bytes` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Monthly Download Allowance Bytes" |  |
| <a id="s-56193b1de6"></a>`timeout_seconds` | no | type="number"; default=300; exclusiveMinimum=0; title="Timeout Seconds" |  |
| <a id="s-8206fec13f"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### <a id="s-7bd5bf1374"></a>definition `CacheStoreDocument`

- <a id="s-92b868a396"></a>`type`: `"object"`
- <a id="s-860fa1d176"></a>`additionalProperties`: `false`
- <a id="s-4b7e31251c"></a>`required`: `["base_url","token_file"]`
- <a id="s-6d1a9e66c3"></a>`title`: `"CacheStoreDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f0a25d79b6"></a>`admission_budget_bytes` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Admission Budget Bytes" |  |
| <a id="s-9a58113585"></a>`admission_enabled` | no | type="boolean"; default=true; title="Admission Enabled" |  |
| <a id="s-67882be5e6"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-6c7eccd29d"></a>`base_url` | yes | type="string"; minLength=1; title="Base Url" |  |
| <a id="s-c550410c2c"></a>`maximum_connections` | no | type="integer"; minimum=1; default=32; title="Maximum Connections" |  |
| <a id="s-fe80260744"></a>`timeout_seconds` | no | type="number"; default=300; exclusiveMinimum=0; title="Timeout Seconds" |  |
| <a id="s-ab6a7b64ad"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### <a id="s-a1fac1c36d"></a>definition `RangePolicyDocument`

- <a id="s-a7ab56b47d"></a>`type`: `"object"`
- <a id="s-208f7d6437"></a>`additionalProperties`: `false`
- <a id="s-53bb505005"></a>`title`: `"RangePolicyDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e73d5d73c7"></a>`billing_mode` | no | type="string"; enum=["returned_bytes","whole_object"]; default="returned_bytes"; title="Billing Mode" |  |
| <a id="s-c1fa43c97b"></a>`max_request_ciphertext_bytes` | no | type="string"; default="67108864"; title="Max Request Ciphertext Bytes" |  |
| <a id="s-df55f50f05"></a>`merge_gap_ciphertext_bytes` | no | type="string"; default="0"; title="Merge Gap Ciphertext Bytes" |  |

### <a id="s-d58ac68fd5"></a>definition `ThroughputDocument`

- <a id="s-f15f207529"></a>`type`: `"object"`
- <a id="s-3522b587e6"></a>`additionalProperties`: `false`
- <a id="s-b30bf918aa"></a>`title`: `"ThroughputDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e52775022d"></a>`age_derivation_concurrency` | no | type="integer"; minimum=1; default=4; title="Age Derivation Concurrency" |  |
| <a id="s-61ca2a0d5c"></a>`age_session_cache_entries` | no | type="integer"; minimum=0; default=128; title="Age Session Cache Entries" |  |
| <a id="s-780e40dfd5"></a>`retrieval_max_inflight_bytes` | no | type="string"; default="1073741824"; title="Retrieval Max Inflight Bytes" |  |
| <a id="s-802d29bd49"></a>`retrieval_read_chunk_bytes` | no | type="string"; default="8388608"; title="Retrieval Read Chunk Bytes" |  |
| <a id="s-c57b55fa82"></a>`retrieval_request_concurrency` | no | type="integer"; minimum=1; default=8; title="Retrieval Request Concurrency" |  |
| <a id="s-2aa027fc61"></a>`source_read_chunk_bytes` | no | type="string"; default="8388608"; title="Source Read Chunk Bytes" |  |
| <a id="s-bda4a54295"></a>`upload_max_inflight_bytes` | no | type="string"; default="1342177280"; title="Upload Max Inflight Bytes" |  |
| <a id="s-5d486cce63"></a>`upload_prepare_concurrency` | no | type="integer"; minimum=1; default=8; title="Upload Prepare Concurrency" |  |
| <a id="s-28a92cc8f0"></a>`upload_request_concurrency` | no | type="integer"; minimum=1; default=4; title="Upload Request Concurrency" |  |
| <a id="s-5d4f746719"></a>`write_concurrency` | no | type="integer"; minimum=1; default=4; title="Write Concurrency" |  |

### <a id="s-9a7157d572"></a>definition `VolumePolicyDocument`

- <a id="s-7ada2cc773"></a>`type`: `"object"`
- <a id="s-f08cc5bc4e"></a>`additionalProperties`: `false`
- <a id="s-a9a3e88dac"></a>`title`: `"VolumePolicyDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b1859dd3ff"></a>`pack_files` | no | type="integer"; minimum=1; default=50000; title="Pack Files" |  |
| <a id="s-a21582bed9"></a>`pack_member_bytes` | no | type="string"; default="16777216"; title="Pack Member Bytes" |  |
| <a id="s-d870426bff"></a>`pack_part_plaintext_bytes` | no | type="string"; default="67108864"; title="Pack Part Plaintext Bytes" |  |
| <a id="s-a4f87e72ab"></a>`pack_source_bytes` | no | type="string"; default="33554432"; title="Pack Source Bytes" |  |
| <a id="s-06f1caa7e2"></a>`raw_part_plaintext_bytes` | no | type="string"; default="67108864"; title="Raw Part Plaintext Bytes" |  |
| <a id="s-751bb27946"></a>`raw_volume_plaintext_bytes` | no | type="string"; default="17179869184"; title="Raw Volume Plaintext Bytes" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/config/riverhog-server.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_passphrase_files](#s-ea597240c6) | `cardinality · entries · operational_policy` | shared above |
| [field archive_read_order](#s-002c9a9bea) | `cardinality · items · operational_policy` | shared above |
| [field archive_stores](#s-854c5e0c07) | `cardinality · entries · operational_policy` | shared above |
| [field range_policy_by_store](#s-4093261341) | `cardinality · entries · operational_policy` | shared above |
| [field retrieval_cache_stores](#s-edb16f7a86) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_scrypt_work_factor](#s-5f3ec0388f) | `value · schema-value · contract_max` | maximum=22 |
| [field catalog_sync_page_size_max](#s-f4b8c66866) | `value · schema-value · contract_max` | maximum=100 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-7997f9fac2"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-e096d2aa15"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-a681b3b3d6"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/config/riverhog-server.schema.json](../../../evidence/sources/authorities.md#src-aae3978953) — [riverhog/src/riverhog\_core/config.schema.json](../../../../../../riverhog/src/riverhog_core/config.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1config~1riverhog-server.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0d70105b8c61d44ed8fcf2eb5865e57069c26540d6c733c114a181573d391b54 -->

```json
{
  "$defs": {
    "ArchiveStoreDocument": {
      "additionalProperties": false,
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "title": "Allow Insecure Http",
          "type": "boolean"
        },
        "base_url": {
          "minLength": 1,
          "title": "Base Url",
          "type": "string"
        },
        "download_safety_buffer_bytes": {
          "default": "0B",
          "title": "Download Safety Buffer Bytes",
          "type": "string"
        },
        "maximum_connections": {
          "default": 32,
          "minimum": 1,
          "title": "Maximum Connections",
          "type": "integer"
        },
        "monthly_download_allowance_bytes": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Monthly Download Allowance Bytes"
        },
        "timeout_seconds": {
          "default": 300,
          "exclusiveMinimum": 0,
          "title": "Timeout Seconds",
          "type": "number"
        },
        "token_file": {
          "format": "path",
          "title": "Token File",
          "type": "string"
        }
      },
      "required": [
        "base_url",
        "token_file"
      ],
      "title": "ArchiveStoreDocument",
      "type": "object"
    },
    "CacheStoreDocument": {
      "additionalProperties": false,
      "properties": {
        "admission_budget_bytes": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Admission Budget Bytes"
        },
        "admission_enabled": {
          "default": true,
          "title": "Admission Enabled",
          "type": "boolean"
        },
        "allow_insecure_http": {
          "default": false,
          "title": "Allow Insecure Http",
          "type": "boolean"
        },
        "base_url": {
          "minLength": 1,
          "title": "Base Url",
          "type": "string"
        },
        "maximum_connections": {
          "default": 32,
          "minimum": 1,
          "title": "Maximum Connections",
          "type": "integer"
        },
        "timeout_seconds": {
          "default": 300,
          "exclusiveMinimum": 0,
          "title": "Timeout Seconds",
          "type": "number"
        },
        "token_file": {
          "format": "path",
          "title": "Token File",
          "type": "string"
        }
      },
      "required": [
        "base_url",
        "token_file"
      ],
      "title": "CacheStoreDocument",
      "type": "object"
    },
    "RangePolicyDocument": {
      "additionalProperties": false,
      "properties": {
        "billing_mode": {
          "default": "returned_bytes",
          "enum": [
            "returned_bytes",
            "whole_object"
          ],
          "title": "Billing Mode",
          "type": "string"
        },
        "max_request_ciphertext_bytes": {
          "default": "67108864",
          "title": "Max Request Ciphertext Bytes",
          "type": "string"
        },
        "merge_gap_ciphertext_bytes": {
          "default": "0",
          "title": "Merge Gap Ciphertext Bytes",
          "type": "string"
        }
      },
      "title": "RangePolicyDocument",
      "type": "object"
    },
    "ThroughputDocument": {
      "additionalProperties": false,
      "properties": {
        "age_derivation_concurrency": {
          "default": 4,
          "minimum": 1,
          "title": "Age Derivation Concurrency",
          "type": "integer"
        },
        "age_session_cache_entries": {
          "default": 128,
          "minimum": 0,
          "title": "Age Session Cache Entries",
          "type": "integer"
        },
        "retrieval_max_inflight_bytes": {
          "default": "1073741824",
          "title": "Retrieval Max Inflight Bytes",
          "type": "string"
        },
        "retrieval_read_chunk_bytes": {
          "default": "8388608",
          "title": "Retrieval Read Chunk Bytes",
          "type": "string"
        },
        "retrieval_request_concurrency": {
          "default": 8,
          "minimum": 1,
          "title": "Retrieval Request Concurrency",
          "type": "integer"
        },
        "source_read_chunk_bytes": {
          "default": "8388608",
          "title": "Source Read Chunk Bytes",
          "type": "string"
        },
        "upload_max_inflight_bytes": {
          "default": "1342177280",
          "title": "Upload Max Inflight Bytes",
          "type": "string"
        },
        "upload_prepare_concurrency": {
          "default": 8,
          "minimum": 1,
          "title": "Upload Prepare Concurrency",
          "type": "integer"
        },
        "upload_request_concurrency": {
          "default": 4,
          "minimum": 1,
          "title": "Upload Request Concurrency",
          "type": "integer"
        },
        "write_concurrency": {
          "default": 4,
          "minimum": 1,
          "title": "Write Concurrency",
          "type": "integer"
        }
      },
      "title": "ThroughputDocument",
      "type": "object"
    },
    "VolumePolicyDocument": {
      "additionalProperties": false,
      "properties": {
        "pack_files": {
          "default": 50000,
          "minimum": 1,
          "title": "Pack Files",
          "type": "integer"
        },
        "pack_member_bytes": {
          "default": "16777216",
          "title": "Pack Member Bytes",
          "type": "string"
        },
        "pack_part_plaintext_bytes": {
          "default": "67108864",
          "title": "Pack Part Plaintext Bytes",
          "type": "string"
        },
        "pack_source_bytes": {
          "default": "33554432",
          "title": "Pack Source Bytes",
          "type": "string"
        },
        "raw_part_plaintext_bytes": {
          "default": "67108864",
          "title": "Raw Part Plaintext Bytes",
          "type": "string"
        },
        "raw_volume_plaintext_bytes": {
          "default": "17179869184",
          "title": "Raw Volume Plaintext Bytes",
          "type": "string"
        }
      },
      "title": "VolumePolicyDocument",
      "type": "object"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/config/riverhog-server.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "archive_active_passphrase_id": {
      "minLength": 1,
      "title": "Archive Active Passphrase Id",
      "type": "string"
    },
    "archive_passphrase_files": {
      "additionalProperties": {
        "format": "path",
        "type": "string"
      },
      "minProperties": 1,
      "title": "Archive Passphrase Files",
      "type": "object"
    },
    "archive_read_order": {
      "items": {
        "type": "string"
      },
      "title": "Archive Read Order",
      "type": "array"
    },
    "archive_scrypt_work_factor": {
      "default": 18,
      "maximum": 22,
      "minimum": 1,
      "title": "Archive Scrypt Work Factor",
      "type": "integer"
    },
    "archive_stores": {
      "additionalProperties": {
        "$ref": "#/$defs/ArchiveStoreDocument"
      },
      "minProperties": 1,
      "title": "Archive Stores",
      "type": "object"
    },
    "archive_upload_sweep_interval": {
      "default": "30s",
      "title": "Archive Upload Sweep Interval",
      "type": "string"
    },
    "archive_write_store": {
      "minLength": 1,
      "title": "Archive Write Store",
      "type": "string"
    },
    "bootstrap_token_file": {
      "format": "path",
      "title": "Bootstrap Token File",
      "type": "string"
    },
    "browse_token_lifetime": {
      "default": "24h",
      "title": "Browse Token Lifetime",
      "type": "string"
    },
    "browse_token_signing_key_file": {
      "format": "path",
      "title": "Browse Token Signing Key File",
      "type": "string"
    },
    "catalog_sync_bootstrap_lifetime": {
      "default": "7d",
      "title": "Catalog Sync Bootstrap Lifetime",
      "type": "string"
    },
    "catalog_sync_cursor_lifetime": {
      "default": "24h",
      "title": "Catalog Sync Cursor Lifetime",
      "type": "string"
    },
    "catalog_sync_history_reap_batch_size": {
      "default": 100,
      "minimum": 1,
      "title": "Catalog Sync History Reap Batch Size",
      "type": "integer"
    },
    "catalog_sync_history_retention": {
      "default": "30d",
      "title": "Catalog Sync History Retention",
      "type": "string"
    },
    "catalog_sync_page_size_max": {
      "default": 100,
      "maximum": 100,
      "minimum": 1,
      "title": "Catalog Sync Page Size Max",
      "type": "integer"
    },
    "collection_upload_custody_lease": {
      "default": "1h",
      "title": "Collection Upload Custody Lease",
      "type": "string"
    },
    "database_url_file": {
      "format": "path",
      "title": "Database Url File",
      "type": "string"
    },
    "event_context_reap_batch_size": {
      "default": 100,
      "minimum": 1,
      "title": "Event Context Reap Batch Size",
      "type": "integer"
    },
    "event_context_retention": {
      "default": "30d",
      "title": "Event Context Retention",
      "type": "string"
    },
    "log_level": {
      "default": "INFO",
      "title": "Log Level",
      "type": "string"
    },
    "public_base_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Public Base Url"
    },
    "range_policy": {
      "$ref": "#/$defs/RangePolicyDocument"
    },
    "range_policy_by_store": {
      "additionalProperties": {
        "$ref": "#/$defs/RangePolicyDocument"
      },
      "title": "Range Policy By Store",
      "type": "object"
    },
    "retrieval_cache_new_archive_enabled": {
      "default": true,
      "title": "Retrieval Cache New Archive Enabled",
      "type": "boolean"
    },
    "retrieval_cache_new_archive_lease": {
      "default": "72h",
      "title": "Retrieval Cache New Archive Lease",
      "type": "string"
    },
    "retrieval_cache_stores": {
      "additionalProperties": {
        "$ref": "#/$defs/CacheStoreDocument"
      },
      "title": "Retrieval Cache Stores",
      "type": "object"
    },
    "retrieval_cache_sweep_interval": {
      "default": "5m",
      "title": "Retrieval Cache Sweep Interval",
      "type": "string"
    },
    "retrieval_cache_write_segment_bytes": {
      "default": "67108864",
      "title": "Retrieval Cache Write Segment Bytes",
      "type": "string"
    },
    "retrieval_default_lease": {
      "default": "24h",
      "title": "Retrieval Default Lease",
      "type": "string"
    },
    "retrieval_estimated_latency": {
      "default": "48h",
      "title": "Retrieval Estimated Latency",
      "type": "string"
    },
    "retrieval_max_lease": {
      "default": "7d",
      "title": "Retrieval Max Lease",
      "type": "string"
    },
    "retrieval_pending_timeout": {
      "default": "72h",
      "title": "Retrieval Pending Timeout",
      "type": "string"
    },
    "retrieval_restore_poll_interval": {
      "default": "5m",
      "title": "Retrieval Restore Poll Interval",
      "type": "string"
    },
    "throughput": {
      "$ref": "#/$defs/ThroughputDocument"
    },
    "volume_policy": {
      "$ref": "#/$defs/VolumePolicyDocument"
    }
  },
  "required": [
    "database_url_file",
    "bootstrap_token_file",
    "browse_token_signing_key_file",
    "archive_passphrase_files",
    "archive_active_passphrase_id",
    "archive_write_store",
    "archive_stores"
  ],
  "title": "RiverhogDocument",
  "type": "object"
}
```

</details>
