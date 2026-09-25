# riverhog-catalog: collection_provenance_verifications

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-provenance-verifications:f6e594743d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-f8df4c117f"></a>

### Table: `collection_provenance_verifications`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-9932270c54"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-2e7b6ebbb8"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-c55acc3fd7"></a>`requested_by_principal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-0e44175f25"></a>`requested_by_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-a1fe7085a7"></a>`requested_at` | `VARCHAR` | no | `—` | — |
| <a id="s-63c188e927"></a>`started_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-2d26de7851"></a>`finished_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-8c80809714"></a>`next_attempt_at` | `VARCHAR` | no | `—` | — |
| <a id="s-fdda0d2250"></a>`attempts` | `INTEGER` | no | `—` | — |
| <a id="s-ebe848d365"></a>`cancel_requested` | `BOOLEAN` | no | `—` | — |
| <a id="s-c147718322"></a>`result_json` | `TEXT` | yes | `—` | — |
| <a id="s-94f64413f2"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-9fadb41d15"></a>`phase` | `VARCHAR` | no | `'metadata'` | — |
| <a id="s-8b6f9442cd"></a>`checkpoint_json` | `TEXT` | no | `'{}'` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-823d9f3cd6"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id)` |
| <a id="s-e8b482321b"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-a782980d78"></a>`check` | `ck_collection_provenance_verifications_state` | `CONSTRAINT ck_collection_provenance_verifications_state CHECK (state IN ('queued','running','canceling','succeeded','failed','canceled'))` |
| <a id="s-af6b431813"></a>`check` | `ck_collection_provenance_verifications_attempts` | `CONSTRAINT ck_collection_provenance_verifications_attempts CHECK (attempts >= 0)` |
| <a id="s-2df6940e8c"></a>`check` | `ck_collection_provenance_verifications_phase` | `CONSTRAINT ck_collection_provenance_verifications_phase CHECK (phase IN ('metadata','identity-tree','identity-bindings','identity-journals','journal-entries','references','reachability','cleanup','complete'))` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-78276d938f"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/25`

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
