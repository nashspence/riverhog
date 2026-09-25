# riverhog-catalog: key_download_usage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-key-download-usage:9f9665b922 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-2587be9bec"></a>

### Table: `key_download_usage`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-5d63dc10db"></a>`key_id` | `VARCHAR` | no | `—` | — |
| <a id="s-03906f154a"></a>`month_started_at` | `VARCHAR` | no | `—` | — |
| <a id="s-0dd7b01539"></a>`accounted_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-981ea06c39"></a>`updated_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-1026e456de"></a>`primary-key` | `—` | `PRIMARY KEY (key_id)` |
| <a id="s-0d2f68e80d"></a>`foreign-key` | `—` | `FOREIGN KEY(key_id) REFERENCES app_keys (id) ON DELETE CASCADE` |
| <a id="s-ed616bb308"></a>`check` | `ck_key_download_usage_bytes` | `CONSTRAINT ck_key_download_usage_bytes CHECK (accounted_bytes >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-458005b547"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/37`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
