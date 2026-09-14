# piggity-local: retrieval_job_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:piggity-local:piggity-local-retrieval-job-files:7ccc0cc556 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity-local](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-71ded8e025"></a>
- Table: `retrieval_job_files`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-6cd4501095"></a>`retrieval_job_id` | `TEXT` | no | `—` | — |
| <a id="s-33612c127b"></a>`ordinal` | `INTEGER` | no | `—` | — |
| <a id="s-32350946e2"></a>`collection_id` | `INTEGER` | no | `—` | — |
| <a id="s-0ad121325f"></a>`path` | `TEXT` | no | `—` | — |
| <a id="s-5cea357901"></a>`bytes` | `INTEGER` | no | `—` | — |
| <a id="s-3766999f0a"></a>`sha256` | `TEXT` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-cc8aaf7cbe"></a>`primary-key` | `—` | `PRIMARY KEY (retrieval_job_id, ordinal)` |
| <a id="s-927ee6eafa"></a>`check` | `ck_retrieval_job_files_ordinal` | `CONSTRAINT ck_retrieval_job_files_ordinal CHECK (ordinal >= 0)` |
| <a id="s-8a27506a2a"></a>`check` | `ck_retrieval_job_files_collection` | `CONSTRAINT ck_retrieval_job_files_collection CHECK (collection_id > 0)` |
| <a id="s-c094da66be"></a>`check` | `ck_retrieval_job_files_bytes` | `CONSTRAINT ck_retrieval_job_files_bytes CHECK (bytes >= 0)` |
| <a id="s-c31400734f"></a>`check` | `ck_retrieval_job_files_sha256` | `CONSTRAINT ck_retrieval_job_files_sha256 CHECK (length(sha256) = 64 AND sha256 = lower(sha256) AND sha256 NOT GLOB '*[^0-9a-f]*')` |
| <a id="s-b15b3515de"></a>`unique` | `uq_retrieval_job_files_artifact` | `CONSTRAINT uq_retrieval_job_files_artifact UNIQUE (retrieval_job_id, collection_id, path)` |
| <a id="s-bbb1b036b6"></a>`foreign-key` | `—` | `FOREIGN KEY(retrieval_job_id) REFERENCES retrieval_jobs (id) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [Schema identity](piggity-local-durable-state-identity.md)

## Governing policies

- <a id="pa-65657b7ffc"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:piggity-local](../../../evidence/sources.md#src-f6a1289f67) — `reference/riverhog/applications/piggity/src/piggity/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/1/structure/tables/5`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bc7242bb5bcfe299af4f711b44fb9ebcca8a4196b938f389aa555e0169e65299 -->

```json
{
  "columns": [
    {
      "definition": "retrieval_job_id TEXT NOT NULL",
      "name": "retrieval_job_id",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "ordinal INTEGER NOT NULL",
      "name": "ordinal",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "collection_id INTEGER NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "path TEXT NOT NULL",
      "name": "path",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "bytes INTEGER NOT NULL",
      "name": "bytes",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "sha256 TEXT NOT NULL",
      "name": "sha256",
      "nullable": false,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "retrieval_job_id",
        "ordinal"
      ],
      "definition": "PRIMARY KEY (retrieval_job_id, ordinal)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_job_files_ordinal CHECK (ordinal >= 0)",
      "expression": "(ordinal >= 0)",
      "kind": "check",
      "name": "ck_retrieval_job_files_ordinal"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_job_files_collection CHECK (collection_id > 0)",
      "expression": "(collection_id > 0)",
      "kind": "check",
      "name": "ck_retrieval_job_files_collection"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_job_files_bytes CHECK (bytes >= 0)",
      "expression": "(bytes >= 0)",
      "kind": "check",
      "name": "ck_retrieval_job_files_bytes"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_job_files_sha256 CHECK (length(sha256) = 64 AND sha256 = lower(sha256) AND sha256 NOT GLOB '*[^0-9a-f]*')",
      "expression": "(length(sha256) = 64 AND sha256 = lower(sha256) AND sha256 NOT GLOB '*[^0-9a-f]*')",
      "kind": "check",
      "name": "ck_retrieval_job_files_sha256"
    },
    {
      "columns": [
        "retrieval_job_id",
        "collection_id",
        "path"
      ],
      "definition": "CONSTRAINT uq_retrieval_job_files_artifact UNIQUE (retrieval_job_id, collection_id, path)",
      "kind": "unique",
      "name": "uq_retrieval_job_files_artifact"
    },
    {
      "columns": [
        "retrieval_job_id"
      ],
      "definition": "FOREIGN KEY(retrieval_job_id) REFERENCES retrieval_jobs (id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "id"
        ],
        "table": "retrieval_jobs"
      }
    }
  ],
  "name": "retrieval_job_files"
}
```
