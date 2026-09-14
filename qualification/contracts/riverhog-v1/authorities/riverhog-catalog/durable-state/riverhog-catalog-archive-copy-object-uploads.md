# riverhog-catalog: archive_copy_object_uploads

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-archive-copy-object-uploads:7510606aa9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-70dff956ac"></a>
- Table: `archive_copy_object_uploads`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-f92835c348"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-96253ac18b"></a>`destination_store` | `VARCHAR` | no | `—` | — |
| <a id="s-866f7ee756"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-601e0242d6"></a>`kind` | `VARCHAR` | no | `—` | — |
| <a id="s-83bd4fc8ad"></a>`object_path` | `VARCHAR` | no | `—` | — |
| <a id="s-2cbe475f98"></a>`plaintext_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-6c15bfa6d3"></a>`sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-a157669199"></a>`write_token` | `VARCHAR` | yes | `—` | — |
| <a id="s-0225d7eda2"></a>`expected_stored_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-7c19b487e0"></a>`uploaded_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-0faba3583a"></a>`uploaded_segments` | `BIGINT` | no | `—` | — |
| <a id="s-b9b185e6c0"></a>`total_segments` | `BIGINT` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-245840b345"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, destination_store, object_id)` |
| <a id="s-177312beb2"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, destination_store) REFERENCES archive_copy_jobs (collection_id, destination_store) ON DELETE CASCADE` |
| <a id="s-9ec756d0bf"></a>`check` | `ck_archive_copy_uploads_plaintext` | `CONSTRAINT ck_archive_copy_uploads_plaintext CHECK (plaintext_bytes >= 0)` |
| <a id="s-f7c514709f"></a>`check` | `ck_archive_copy_uploads_uploaded_bytes` | `CONSTRAINT ck_archive_copy_uploads_uploaded_bytes CHECK (uploaded_bytes >= 0)` |
| <a id="s-ebef67aaac"></a>`check` | `ck_archive_copy_uploads_uploaded_segments` | `CONSTRAINT ck_archive_copy_uploads_uploaded_segments CHECK (uploaded_segments >= 0)` |
| <a id="s-9a56894cb5"></a>`check` | `ck_archive_copy_uploads_total_segments` | `CONSTRAINT ck_archive_copy_uploads_total_segments CHECK (total_segments >= 0)` |
| <a id="s-178bede1ea"></a>`check` | `ck_archive_copy_uploads_segment_progress` | `CONSTRAINT ck_archive_copy_uploads_segment_progress CHECK (uploaded_segments <= total_segments)` |
| <a id="s-6204018147"></a>`check` | `ck_archive_copy_object_uploads_sha256_hex` | `CONSTRAINT ck_archive_copy_object_uploads_sha256_hex CHECK (sha256 IS NULL OR length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-219ac3ddf2"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/68`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f28dfed15a8dac5572b23d12e0c33849e3b2d6623e5630612fc9b1c7cdf68a0b -->

```json
{
  "columns": [
    {
      "definition": "collection_id BIGINT NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "destination_store VARCHAR NOT NULL",
      "name": "destination_store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "object_id VARCHAR NOT NULL",
      "name": "object_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "kind VARCHAR NOT NULL",
      "name": "kind",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "object_path VARCHAR NOT NULL",
      "name": "object_path",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "plaintext_bytes BIGINT NOT NULL",
      "name": "plaintext_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "sha256 VARCHAR(64)",
      "name": "sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "write_token VARCHAR",
      "name": "write_token",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "expected_stored_bytes BIGINT",
      "name": "expected_stored_bytes",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "uploaded_bytes BIGINT NOT NULL",
      "name": "uploaded_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "uploaded_segments BIGINT NOT NULL",
      "name": "uploaded_segments",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "total_segments BIGINT NOT NULL",
      "name": "total_segments",
      "nullable": false,
      "type": "BIGINT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "destination_store",
        "object_id"
      ],
      "definition": "PRIMARY KEY (collection_id, destination_store, object_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id",
        "destination_store"
      ],
      "definition": "FOREIGN KEY(collection_id, destination_store) REFERENCES archive_copy_jobs (collection_id, destination_store) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "destination_store"
        ],
        "table": "archive_copy_jobs"
      }
    },
    {
      "definition": "CONSTRAINT ck_archive_copy_uploads_plaintext CHECK (plaintext_bytes >= 0)",
      "expression": "(plaintext_bytes >= 0)",
      "kind": "check",
      "name": "ck_archive_copy_uploads_plaintext"
    },
    {
      "definition": "CONSTRAINT ck_archive_copy_uploads_uploaded_bytes CHECK (uploaded_bytes >= 0)",
      "expression": "(uploaded_bytes >= 0)",
      "kind": "check",
      "name": "ck_archive_copy_uploads_uploaded_bytes"
    },
    {
      "definition": "CONSTRAINT ck_archive_copy_uploads_uploaded_segments CHECK (uploaded_segments >= 0)",
      "expression": "(uploaded_segments >= 0)",
      "kind": "check",
      "name": "ck_archive_copy_uploads_uploaded_segments"
    },
    {
      "definition": "CONSTRAINT ck_archive_copy_uploads_total_segments CHECK (total_segments >= 0)",
      "expression": "(total_segments >= 0)",
      "kind": "check",
      "name": "ck_archive_copy_uploads_total_segments"
    },
    {
      "definition": "CONSTRAINT ck_archive_copy_uploads_segment_progress CHECK (uploaded_segments <= total_segments)",
      "expression": "(uploaded_segments <= total_segments)",
      "kind": "check",
      "name": "ck_archive_copy_uploads_segment_progress"
    },
    {
      "definition": "CONSTRAINT ck_archive_copy_object_uploads_sha256_hex CHECK (sha256 IS NULL OR length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(sha256 IS NULL OR length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_archive_copy_object_uploads_sha256_hex"
    }
  ],
  "name": "archive_copy_object_uploads"
}
```
