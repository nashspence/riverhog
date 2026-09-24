# riverhog-catalog: collection_provenance_verifications

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-provenance-verifications:5745202730 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-41a27eabce"></a>

### Table: `collection_provenance_verifications`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-223457f6a8"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-336a144acc"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-4137247875"></a>`requested_by_principal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-def11e2d0b"></a>`requested_by_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-34f95eee04"></a>`requested_at` | `VARCHAR` | no | `—` | — |
| <a id="s-4ed9a280ee"></a>`started_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-68308b08da"></a>`finished_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-b4167b202b"></a>`next_attempt_at` | `VARCHAR` | no | `—` | — |
| <a id="s-febe25e0e6"></a>`attempts` | `INTEGER` | no | `—` | — |
| <a id="s-afacd708bc"></a>`cancel_requested` | `BOOLEAN` | no | `—` | — |
| <a id="s-8ed8e2a3c7"></a>`result_json` | `TEXT` | yes | `—` | — |
| <a id="s-f86de28e76"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-8c3adb7a28"></a>`phase` | `VARCHAR` | no | `'metadata'` | — |
| <a id="s-ef6c6b8c98"></a>`checkpoint_json` | `TEXT` | no | `'{}'` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-f6bebec933"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id)` |
| <a id="s-742acfcd0e"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-9c4daf586c"></a>`check` | `ck_collection_provenance_verifications_state` | `CONSTRAINT ck_collection_provenance_verifications_state CHECK (state IN ('queued','running','canceling','succeeded','failed','canceled'))` |
| <a id="s-ca4308bcf6"></a>`check` | `ck_collection_provenance_verifications_attempts` | `CONSTRAINT ck_collection_provenance_verifications_attempts CHECK (attempts >= 0)` |
| <a id="s-d3140f161a"></a>`check` | `ck_collection_provenance_verifications_phase` | `CONSTRAINT ck_collection_provenance_verifications_phase CHECK (phase IN ('metadata','identity-tree','identity-bindings','identity-journals','journal-entries','references','reachability','cleanup','complete'))` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-c384b18610"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/24`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3358666635bc30d5833b92f345a4ba42e92ed18846c681d48c23a11cee3b8305 -->

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
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "requested_by_principal_id VARCHAR NOT NULL",
      "name": "requested_by_principal_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "requested_by_key_id VARCHAR",
      "name": "requested_by_key_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "requested_at VARCHAR NOT NULL",
      "name": "requested_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "started_at VARCHAR",
      "name": "started_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "finished_at VARCHAR",
      "name": "finished_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "next_attempt_at VARCHAR NOT NULL",
      "name": "next_attempt_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "attempts INTEGER NOT NULL",
      "name": "attempts",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "cancel_requested BOOLEAN NOT NULL",
      "name": "cancel_requested",
      "nullable": false,
      "type": "BOOLEAN"
    },
    {
      "definition": "result_json TEXT",
      "name": "result_json",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "failure TEXT",
      "name": "failure",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "default": "'metadata'",
      "definition": "phase VARCHAR DEFAULT 'metadata' NOT NULL",
      "name": "phase",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "default": "'{}'",
      "definition": "checkpoint_json TEXT DEFAULT '{}' NOT NULL",
      "name": "checkpoint_json",
      "nullable": false,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id"
      ],
      "definition": "PRIMARY KEY (collection_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id"
      ],
      "definition": "FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "id"
        ],
        "table": "collections"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_provenance_verifications_state CHECK (state IN ('queued','running','canceling','succeeded','failed','canceled'))",
      "expression": "(state IN ('queued','running','canceling','succeeded','failed','canceled'))",
      "kind": "check",
      "name": "ck_collection_provenance_verifications_state"
    },
    {
      "definition": "CONSTRAINT ck_collection_provenance_verifications_attempts CHECK (attempts >= 0)",
      "expression": "(attempts >= 0)",
      "kind": "check",
      "name": "ck_collection_provenance_verifications_attempts"
    },
    {
      "definition": "CONSTRAINT ck_collection_provenance_verifications_phase CHECK (phase IN ('metadata','identity-tree','identity-bindings','identity-journals','journal-entries','references','reachability','cleanup','complete'))",
      "expression": "(phase IN ('metadata','identity-tree','identity-bindings','identity-journals','journal-entries','references','reachability','cleanup','complete'))",
      "kind": "check",
      "name": "ck_collection_provenance_verifications_phase"
    }
  ],
  "name": "collection_provenance_verifications"
}
```

</details>
