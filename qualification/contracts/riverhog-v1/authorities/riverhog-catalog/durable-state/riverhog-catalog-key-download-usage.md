# riverhog-catalog: key_download_usage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-key-download-usage:c83696a09d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-fa263b79dc"></a>
- Table: `key_download_usage`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-7863a39505"></a>`key_id` | `VARCHAR` | no | `—` | — |
| <a id="s-177ea34b48"></a>`month_started_at` | `VARCHAR` | no | `—` | — |
| <a id="s-e6de94c392"></a>`accounted_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-38e846e76c"></a>`updated_at` | `VARCHAR` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-151bd8cc62"></a>`primary-key` | `—` | `PRIMARY KEY (key_id)` |
| <a id="s-0a5f196f99"></a>`foreign-key` | `—` | `FOREIGN KEY(key_id) REFERENCES app_keys (id) ON DELETE CASCADE` |
| <a id="s-773cb4497a"></a>`check` | `ck_key_download_usage_bytes` | `CONSTRAINT ck_key_download_usage_bytes CHECK (accounted_bytes >= 0)` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-8b4671f4d0"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/35`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c68e7cffa51917a9af09d602a1befa51871168fa289365f88dff0a144c5672c -->

```json
{
  "columns": [
    {
      "definition": "key_id VARCHAR NOT NULL",
      "name": "key_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "month_started_at VARCHAR NOT NULL",
      "name": "month_started_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "accounted_bytes BIGINT NOT NULL",
      "name": "accounted_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "updated_at VARCHAR NOT NULL",
      "name": "updated_at",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "key_id"
      ],
      "definition": "PRIMARY KEY (key_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "key_id"
      ],
      "definition": "FOREIGN KEY(key_id) REFERENCES app_keys (id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "id"
        ],
        "table": "app_keys"
      }
    },
    {
      "definition": "CONSTRAINT ck_key_download_usage_bytes CHECK (accounted_bytes >= 0)",
      "expression": "(accounted_bytes >= 0)",
      "kind": "check",
      "name": "ck_key_download_usage_bytes"
    }
  ],
  "name": "key_download_usage"
}
```
