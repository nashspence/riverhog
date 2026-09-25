# riverhog-catalog: collection_upload_copy_intents

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-copy-intents:e0c1a6f5ad -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-9f17464520"></a>

### Table: `collection_upload_copy_intents`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1683c8820f"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-f399da7285"></a>`destination_store` | `VARCHAR` | no | `—` | — |
| <a id="s-b3e44d0509"></a>`destination_incarnation_id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-be2edcf9a0"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-cd859c2958"></a>`source_incarnation_id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-cf6f09bdb4"></a>`initiated_by_app` | `VARCHAR` | no | `—` | — |
| <a id="s-60a24873ef"></a>`initiated_by_key_id` | `VARCHAR` | no | `—` | — |
| <a id="s-3e8e3b39be"></a>`event_context_json` | `TEXT` | yes | `—` | — |
| <a id="s-c22939e57b"></a>`use_cache` | `BOOLEAN` | no | `—` | — |
| <a id="s-c7ddb3cc39"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-4781da89b7"></a>`accepted_at` | `VARCHAR` | no | `—` | — |
| <a id="s-3ae8a4b083"></a>`next_attempt_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-a3c200b128"></a>`attempts` | `INTEGER` | no | `—` | — |
| <a id="s-770ae5b08d"></a>`handed_off_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-f292b70346"></a>`job_created` | `BOOLEAN` | yes | `—` | — |
| <a id="s-4fe82066b8"></a>`failure_code` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-a77d7bc5f1"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, destination_store)` |
| <a id="s-3f1ce690b9"></a>`foreign-key` | `—` | `FOREIGN KEY(source_incarnation_id, source_store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-98ec8eb1b1"></a>`foreign-key` | `—` | `FOREIGN KEY(destination_incarnation_id, destination_store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-6be6d760af"></a>`check` | `ck_collection_upload_copy_intents_state` | `CONSTRAINT ck_collection_upload_copy_intents_state CHECK (state IN ('accepted','pending','handed_off','failed','canceled'))` |
| <a id="s-6074530449"></a>`check` | `ck_collection_upload_copy_intents_attempts` | `CONSTRAINT ck_collection_upload_copy_intents_attempts CHECK (attempts >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-1548dddc28"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/11`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d07fb819b52eb2f1c9919c8ff256dda5e59580f961c8546291fdcadd747b201c -->

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
      "definition": "destination_incarnation_id VARCHAR(36) NOT NULL",
      "name": "destination_incarnation_id",
      "nullable": false,
      "type": "VARCHAR(36)"
    },
    {
      "definition": "source_store VARCHAR NOT NULL",
      "name": "source_store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "source_incarnation_id VARCHAR(36) NOT NULL",
      "name": "source_incarnation_id",
      "nullable": false,
      "type": "VARCHAR(36)"
    },
    {
      "definition": "initiated_by_app VARCHAR NOT NULL",
      "name": "initiated_by_app",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "initiated_by_key_id VARCHAR NOT NULL",
      "name": "initiated_by_key_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "event_context_json TEXT",
      "name": "event_context_json",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "use_cache BOOLEAN NOT NULL",
      "name": "use_cache",
      "nullable": false,
      "type": "BOOLEAN"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "accepted_at VARCHAR NOT NULL",
      "name": "accepted_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "next_attempt_at VARCHAR",
      "name": "next_attempt_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "attempts INTEGER NOT NULL",
      "name": "attempts",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "handed_off_at VARCHAR",
      "name": "handed_off_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "job_created BOOLEAN",
      "name": "job_created",
      "nullable": true,
      "type": "BOOLEAN"
    },
    {
      "definition": "failure_code VARCHAR",
      "name": "failure_code",
      "nullable": true,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "destination_store"
      ],
      "definition": "PRIMARY KEY (collection_id, destination_store)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "source_incarnation_id",
        "source_store"
      ],
      "definition": "FOREIGN KEY(source_incarnation_id, source_store) REFERENCES storage_incarnations (id, name)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "id",
          "name"
        ],
        "table": "storage_incarnations"
      }
    },
    {
      "columns": [
        "destination_incarnation_id",
        "destination_store"
      ],
      "definition": "FOREIGN KEY(destination_incarnation_id, destination_store) REFERENCES storage_incarnations (id, name)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "id",
          "name"
        ],
        "table": "storage_incarnations"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_copy_intents_state CHECK (state IN ('accepted','pending','handed_off','failed','canceled'))",
      "expression": "(state IN ('accepted','pending','handed_off','failed','canceled'))",
      "kind": "check",
      "name": "ck_collection_upload_copy_intents_state"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_copy_intents_attempts CHECK (attempts >= 0)",
      "expression": "(attempts >= 0)",
      "kind": "check",
      "name": "ck_collection_upload_copy_intents_attempts"
    }
  ],
  "name": "collection_upload_copy_intents"
}
```

</details>
