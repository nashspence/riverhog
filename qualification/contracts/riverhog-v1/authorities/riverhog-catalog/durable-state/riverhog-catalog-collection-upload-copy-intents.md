# riverhog-catalog: collection_upload_copy_intents

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-copy-intents:dbd9f3e9b5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-0dcdf28a9e"></a>

### Table: `collection_upload_copy_intents`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-d601b16a2d"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-8d00d34a79"></a>`destination_store` | `VARCHAR` | no | `—` | — |
| <a id="s-bff7e96752"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-5fc246700a"></a>`destination_binding_sha256` | `VARCHAR` | no | `—` | — |
| <a id="s-a48ee9c9b4"></a>`source_binding_sha256` | `VARCHAR` | no | `—` | — |
| <a id="s-384daf9286"></a>`initiated_by_app` | `VARCHAR` | no | `—` | — |
| <a id="s-280d24f4aa"></a>`initiated_by_key_id` | `VARCHAR` | no | `—` | — |
| <a id="s-b0c3b1b8cc"></a>`event_context_json` | `TEXT` | yes | `—` | — |
| <a id="s-bb5ce35505"></a>`use_cache` | `BOOLEAN` | no | `—` | — |
| <a id="s-be3c5e03ab"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-ed5bd755d3"></a>`accepted_at` | `VARCHAR` | no | `—` | — |
| <a id="s-00862a3d88"></a>`next_attempt_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-85a482cece"></a>`attempts` | `INTEGER` | no | `—` | — |
| <a id="s-b4db2f1365"></a>`handed_off_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-8886a5121a"></a>`job_created` | `BOOLEAN` | yes | `—` | — |
| <a id="s-a1cc0f61ff"></a>`failure_code` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-2bda372a88"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, destination_store)` |
| <a id="s-0c435c0da4"></a>`check` | `ck_collection_upload_copy_intents_state` | `CONSTRAINT ck_collection_upload_copy_intents_state CHECK (state IN ('accepted','pending','handed_off','failed','canceled'))` |
| <a id="s-c6d82b9c25"></a>`check` | `ck_collection_upload_copy_intents_attempts` | `CONSTRAINT ck_collection_upload_copy_intents_attempts CHECK (attempts >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-e2a0a464f4"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/10`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: abe66a2fdd8f97652b9a463b05de726b69ee5345e28ae3106c624c97f7aecc39 -->

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
      "definition": "source_store VARCHAR NOT NULL",
      "name": "source_store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "destination_binding_sha256 VARCHAR NOT NULL",
      "name": "destination_binding_sha256",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "source_binding_sha256 VARCHAR NOT NULL",
      "name": "source_binding_sha256",
      "nullable": false,
      "type": "VARCHAR"
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
