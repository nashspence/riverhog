# riverhog-server:configuration:riverhog-document configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:riverhog-server:riverhog-server-configuration-riverhog-do-0d09d16e15:14e0386206 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-4d8429895b"></a>

- <a id="s-3c7350a91f"></a>`type`: `"object"`
- <a id="s-42ad6f3119"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/riverhog-server.schema.json"`
- <a id="s-fee596d151"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-3cfbfc105c"></a>`additionalProperties`: `false`
- <a id="s-27a6ea68ee"></a>`required`: `["database_url_file","bootstrap_token_file","browse_token_signing_key_file","archive_passphrase_files","archive_active_passphrase_id","archive_write_store","archive_stores"]`
- <a id="s-a8394c6956"></a>`title`: `"RiverhogDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3164fb4708"></a>`archive_active_passphrase_id` | yes | type="string"; minLength=1; title="Archive Active Passphrase Id" |  |
| <a id="s-7781abc90f"></a>`archive_passphrase_files` | yes | type="object"; additionalProperties=(type="string"; format="path"); minProperties=1; title="Archive Passphrase Files" |  |
| <a id="s-92ff3e9aeb"></a>`archive_read_order` | no | type="array"; items=(type="string"); title="Archive Read Order" |  |
| <a id="s-e0f8260698"></a>`archive_scrypt_work_factor` | no | type="integer"; minimum=1; maximum=22; default=18; title="Archive Scrypt Work Factor" |  |
| <a id="s-6d244211ea"></a>`archive_stores` | yes | type="object"; additionalProperties=([ArchiveStoreDocument](#s-6b341dd231)); minProperties=1; title="Archive Stores" |  |
| <a id="s-76d6176fea"></a>`archive_upload_sweep_interval` | no | type="string"; default="30s"; title="Archive Upload Sweep Interval" |  |
| <a id="s-1be89f3d85"></a>`archive_write_store` | yes | type="string"; minLength=1; title="Archive Write Store" |  |
| <a id="s-19a39a2d37"></a>`bootstrap_token_file` | yes | type="string"; format="path"; title="Bootstrap Token File" |  |
| <a id="s-b635ca1676"></a>`browse_token_lifetime` | no | type="string"; default="24h"; title="Browse Token Lifetime" |  |
| <a id="s-f002b303db"></a>`browse_token_signing_key_file` | yes | type="string"; format="path"; title="Browse Token Signing Key File" |  |
| <a id="s-ecb3665523"></a>`catalog_sync_bootstrap_lifetime` | no | type="string"; default="7d"; title="Catalog Sync Bootstrap Lifetime" |  |
| <a id="s-4dac68f3e9"></a>`catalog_sync_cursor_lifetime` | no | type="string"; default="24h"; title="Catalog Sync Cursor Lifetime" |  |
| <a id="s-003c67ac37"></a>`catalog_sync_history_reap_batch_size` | no | type="integer"; minimum=1; default=100; title="Catalog Sync History Reap Batch Size" |  |
| <a id="s-534c960b99"></a>`catalog_sync_history_retention` | no | type="string"; default="30d"; title="Catalog Sync History Retention" |  |
| <a id="s-7c17017719"></a>`catalog_sync_page_size_max` | no | type="integer"; minimum=1; maximum=100; default=100; title="Catalog Sync Page Size Max" |  |
| <a id="s-ab4037fd34"></a>`collection_upload_custody_lease` | no | type="string"; default="1h"; title="Collection Upload Custody Lease" |  |
| <a id="s-5031198b9f"></a>`database_url_file` | yes | type="string"; format="path"; title="Database Url File" |  |
| <a id="s-5da15212c6"></a>`event_context_reap_batch_size` | no | type="integer"; minimum=1; default=100; title="Event Context Reap Batch Size" |  |
| <a id="s-3762da046b"></a>`event_context_retention` | no | type="string"; default="30d"; title="Event Context Retention" |  |
| <a id="s-394fe654a2"></a>`log_level` | no | type="string"; default="INFO"; title="Log Level" |  |
| <a id="s-4fc756f701"></a>`public_base_url` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Public Base Url" |  |
| <a id="s-9383551c7a"></a>`range_policy` | no | [RangePolicyDocument](#s-1fa6a56a94) |  |
| <a id="s-3e07943850"></a>`range_policy_by_store` | no | type="object"; additionalProperties=([RangePolicyDocument](#s-1fa6a56a94)); title="Range Policy By Store" |  |
| <a id="s-f31a9f1de2"></a>`retrieval_cache_new_archive_enabled` | no | type="boolean"; default=true; title="Retrieval Cache New Archive Enabled" |  |
| <a id="s-f7a62e3973"></a>`retrieval_cache_new_archive_lease` | no | type="string"; default="72h"; title="Retrieval Cache New Archive Lease" |  |
| <a id="s-8c5fd6f18a"></a>`retrieval_cache_stores` | no | type="object"; additionalProperties=([CacheStoreDocument](#s-6e6884de0e)); title="Retrieval Cache Stores" |  |
| <a id="s-8af16f83ae"></a>`retrieval_cache_sweep_interval` | no | type="string"; default="5m"; title="Retrieval Cache Sweep Interval" |  |
| <a id="s-f67adabf5b"></a>`retrieval_cache_write_segment_bytes` | no | type="string"; default="67108864"; title="Retrieval Cache Write Segment Bytes" |  |
| <a id="s-1117c46a63"></a>`retrieval_default_lease` | no | type="string"; default="24h"; title="Retrieval Default Lease" |  |
| <a id="s-0f61bb34a4"></a>`retrieval_estimated_latency` | no | type="string"; default="48h"; title="Retrieval Estimated Latency" |  |
| <a id="s-4e3adc164a"></a>`retrieval_max_lease` | no | type="string"; default="7d"; title="Retrieval Max Lease" |  |
| <a id="s-216e7cb372"></a>`retrieval_pending_timeout` | no | type="string"; default="72h"; title="Retrieval Pending Timeout" |  |
| <a id="s-917af01cdc"></a>`retrieval_restore_poll_interval` | no | type="string"; default="5m"; title="Retrieval Restore Poll Interval" |  |
| <a id="s-2b0e5267f9"></a>`throughput` | no | [ThroughputDocument](#s-a59cfb2bbe) |  |
| <a id="s-1dab6a8bb1"></a>`volume_policy` | no | [VolumePolicyDocument](#s-a067aa6408) |  |

### Definitions

- [ArchiveStoreDocument](#s-6b341dd231)
- [CacheStoreDocument](#s-6e6884de0e)
- [RangePolicyDocument](#s-1fa6a56a94)
- [ThroughputDocument](#s-a59cfb2bbe)
- [VolumePolicyDocument](#s-a067aa6408)

### <a id="s-6b341dd231"></a>definition `ArchiveStoreDocument`

- <a id="s-0b1d9b7c1d"></a>`type`: `"object"`
- <a id="s-b1e990f34f"></a>`additionalProperties`: `false`
- <a id="s-6288036ba3"></a>`required`: `["base_url","token_file"]`
- <a id="s-369706b615"></a>`title`: `"ArchiveStoreDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9b7649678d"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-3407c78863"></a>`base_url` | yes | type="string"; minLength=1; title="Base Url" |  |
| <a id="s-e6d293441e"></a>`download_safety_buffer_bytes` | no | type="string"; default="0B"; title="Download Safety Buffer Bytes" |  |
| <a id="s-b2cd714b9c"></a>`maximum_connections` | no | type="integer"; minimum=1; default=32; title="Maximum Connections" |  |
| <a id="s-636d9e15bb"></a>`monthly_download_allowance_bytes` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Monthly Download Allowance Bytes" |  |
| <a id="s-4f122bfeb0"></a>`timeout_seconds` | no | type="number"; default=300; exclusiveMinimum=0; title="Timeout Seconds" |  |
| <a id="s-35d4d6abcb"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### <a id="s-6e6884de0e"></a>definition `CacheStoreDocument`

- <a id="s-b79ee76aaa"></a>`type`: `"object"`
- <a id="s-4eb4459834"></a>`additionalProperties`: `false`
- <a id="s-8bc72a9ae2"></a>`required`: `["base_url","token_file"]`
- <a id="s-b11b97b6eb"></a>`title`: `"CacheStoreDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5042d7ee72"></a>`admission_budget_bytes` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Admission Budget Bytes" |  |
| <a id="s-36a6476b16"></a>`admission_enabled` | no | type="boolean"; default=true; title="Admission Enabled" |  |
| <a id="s-f33c34d83b"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-6b05a21a72"></a>`base_url` | yes | type="string"; minLength=1; title="Base Url" |  |
| <a id="s-03103c93dc"></a>`maximum_connections` | no | type="integer"; minimum=1; default=32; title="Maximum Connections" |  |
| <a id="s-2492537afe"></a>`timeout_seconds` | no | type="number"; default=300; exclusiveMinimum=0; title="Timeout Seconds" |  |
| <a id="s-1c0b4e33d7"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### <a id="s-1fa6a56a94"></a>definition `RangePolicyDocument`

- <a id="s-9a41d1a0d0"></a>`type`: `"object"`
- <a id="s-3fab9822cb"></a>`additionalProperties`: `false`
- <a id="s-29c793546e"></a>`title`: `"RangePolicyDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-507f7922f8"></a>`billing_mode` | no | type="string"; enum=["returned_bytes","whole_object"]; default="returned_bytes"; title="Billing Mode" |  |
| <a id="s-f99cb1b504"></a>`max_request_ciphertext_bytes` | no | type="string"; default="67108864"; title="Max Request Ciphertext Bytes" |  |
| <a id="s-cb4fd968fe"></a>`merge_gap_ciphertext_bytes` | no | type="string"; default="0"; title="Merge Gap Ciphertext Bytes" |  |

### <a id="s-a59cfb2bbe"></a>definition `ThroughputDocument`

- <a id="s-ce92b1226d"></a>`type`: `"object"`
- <a id="s-5755293b78"></a>`additionalProperties`: `false`
- <a id="s-8b5950b570"></a>`title`: `"ThroughputDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-18b5fe36d1"></a>`age_derivation_concurrency` | no | type="integer"; minimum=1; default=4; title="Age Derivation Concurrency" |  |
| <a id="s-88432a069c"></a>`age_session_cache_entries` | no | type="integer"; minimum=0; default=128; title="Age Session Cache Entries" |  |
| <a id="s-35052ac5e0"></a>`retrieval_max_inflight_bytes` | no | type="string"; default="1073741824"; title="Retrieval Max Inflight Bytes" |  |
| <a id="s-ea8840c856"></a>`retrieval_read_chunk_bytes` | no | type="string"; default="8388608"; title="Retrieval Read Chunk Bytes" |  |
| <a id="s-6556add56d"></a>`retrieval_request_concurrency` | no | type="integer"; minimum=1; default=8; title="Retrieval Request Concurrency" |  |
| <a id="s-6a97c2b53b"></a>`source_read_chunk_bytes` | no | type="string"; default="8388608"; title="Source Read Chunk Bytes" |  |
| <a id="s-3a356c64b6"></a>`upload_max_inflight_bytes` | no | type="string"; default="1342177280"; title="Upload Max Inflight Bytes" |  |
| <a id="s-b2efc4efbe"></a>`upload_prepare_concurrency` | no | type="integer"; minimum=1; default=8; title="Upload Prepare Concurrency" |  |
| <a id="s-0c31c95277"></a>`upload_request_concurrency` | no | type="integer"; minimum=1; default=4; title="Upload Request Concurrency" |  |
| <a id="s-5620d4b3eb"></a>`write_concurrency` | no | type="integer"; minimum=1; default=4; title="Write Concurrency" |  |

### <a id="s-a067aa6408"></a>definition `VolumePolicyDocument`

- <a id="s-73676fe34f"></a>`type`: `"object"`
- <a id="s-13b0441e2d"></a>`additionalProperties`: `false`
- <a id="s-ece8651064"></a>`title`: `"VolumePolicyDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-69547355d0"></a>`pack_files` | no | type="integer"; minimum=1; default=50000; title="Pack Files" |  |
| <a id="s-2ce842fb99"></a>`pack_member_bytes` | no | type="string"; default="16777216"; title="Pack Member Bytes" |  |
| <a id="s-103f1aae9f"></a>`pack_part_plaintext_bytes` | no | type="string"; default="67108864"; title="Pack Part Plaintext Bytes" |  |
| <a id="s-fbb0a9e31e"></a>`pack_source_bytes` | no | type="string"; default="33554432"; title="Pack Source Bytes" |  |
| <a id="s-58cdeedbfc"></a>`raw_part_plaintext_bytes` | no | type="string"; default="67108864"; title="Raw Part Plaintext Bytes" |  |
| <a id="s-83ddc1cf02"></a>`raw_volume_plaintext_bytes` | no | type="string"; default="17179869184"; title="Raw Volume Plaintext Bytes" |  |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-server:configuration:riverhog-document"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_passphrase_files](#s-7781abc90f) | `cardinality · entries · operational_policy` | shared above |
| [field archive_read_order](#s-92ff3e9aeb) | `cardinality · items · operational_policy` | shared above |
| [field archive_stores](#s-6d244211ea) | `cardinality · entries · operational_policy` | shared above |
| [field range_policy_by_store](#s-3e07943850) | `cardinality · entries · operational_policy` | shared above |
| [field retrieval_cache_stores](#s-8c5fd6f18a) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_scrypt_work_factor](#s-e0f8260698) | `value · schema-value · contract_max` | maximum=22 |
| [field catalog_sync_page_size_max](#s-7c17017719) | `value · schema-value · contract_max` | maximum=100 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-00f741d7f5"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-68d3ea4334"></a>[extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)
- <a id="pa-1f550eb2b2"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:riverhog-server:configuration:riverhog-document](../../../evidence/sources/authorities.md#src-3775bab45e) — [riverhog/src/riverhog\_core/runtime\_document.py::RiverhogDocument](../../../../../../riverhog/src/riverhog_core/runtime_document.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/riverhog-server:configuration:riverhog-document`

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
