# riverhog-catalog: key_download_reservations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-key-download-reservations:b7ac054a14 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-6ca2e182f7"></a>

### Table: `key_download_reservations`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-3db65fc7b0"></a>`id` | `VARCHAR` | no | `—` | — |
| <a id="s-ef924f4d34"></a>`key_id` | `VARCHAR` | no | `—` | — |
| <a id="s-70eca3d278"></a>`job_id` | `VARCHAR` | no | `—` | — |
| <a id="s-f2b960715a"></a>`kind` | `VARCHAR` | no | `—` | — |
| <a id="s-1ed2e005fb"></a>`month_started_at` | `VARCHAR` | no | `—` | — |
| <a id="s-f9db2bf152"></a>`reserved_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-ccdb773751"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-e9bd0638ac"></a>`expires_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-f30d6f28b6"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-f932533996"></a>`foreign-key` | `—` | `FOREIGN KEY(key_id) REFERENCES app_keys (id) ON DELETE CASCADE` |
| <a id="s-4a7bd078df"></a>`check` | `ck_key_download_reservations_kind` | `CONSTRAINT ck_key_download_reservations_kind CHECK (kind IN ('job','stream'))` |
| <a id="s-1415db01e5"></a>`check` | `ck_key_download_reservations_bytes` | `CONSTRAINT ck_key_download_reservations_bytes CHECK (reserved_bytes >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-ba48ff03ed"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/34`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6a5ad4e8173e5e0a6beded721c7d6b037467d89a406ff25d594ac56d3aa286e7 -->

```json
{
  "columns": [
    {
      "definition": "id VARCHAR NOT NULL",
      "name": "id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "key_id VARCHAR NOT NULL",
      "name": "key_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "job_id VARCHAR NOT NULL",
      "name": "job_id",
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
      "definition": "month_started_at VARCHAR NOT NULL",
      "name": "month_started_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "reserved_bytes BIGINT NOT NULL",
      "name": "reserved_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "expires_at VARCHAR NOT NULL",
      "name": "expires_at",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "id"
      ],
      "definition": "PRIMARY KEY (id)",
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
      "definition": "CONSTRAINT ck_key_download_reservations_kind CHECK (kind IN ('job','stream'))",
      "expression": "(kind IN ('job','stream'))",
      "kind": "check",
      "name": "ck_key_download_reservations_kind"
    },
    {
      "definition": "CONSTRAINT ck_key_download_reservations_bytes CHECK (reserved_bytes >= 0)",
      "expression": "(reserved_bytes >= 0)",
      "kind": "check",
      "name": "ck_key_download_reservations_bytes"
    }
  ],
  "name": "key_download_reservations"
}
```

</details>
