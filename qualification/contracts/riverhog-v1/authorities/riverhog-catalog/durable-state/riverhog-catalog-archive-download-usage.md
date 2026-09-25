# riverhog-catalog: archive_download_usage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-archive-download-usage:8c05804459 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-fa8f69a215"></a>

### Table: `archive_download_usage`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1e52ad9c40"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-1e5460704b"></a>`month_started_at` | `VARCHAR` | no | `—` | — |
| <a id="s-0898bbdc0b"></a>`accounted_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-9af8ae4edd"></a>`updated_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-a5cb3a6dcb"></a>`primary-key` | `—` | `PRIMARY KEY (store)` |
| <a id="s-999db9919c"></a>`check` | `ck_archive_download_usage_bytes` | `CONSTRAINT ck_archive_download_usage_bytes CHECK (accounted_bytes >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-f787e8c4a6"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/2`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8a52bad0a23fd3395262e8da3b661ae39d585274913434c593e404526b6f9fa1 -->

```json
{
  "columns": [
    {
      "definition": "store VARCHAR NOT NULL",
      "name": "store",
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
        "store"
      ],
      "definition": "PRIMARY KEY (store)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_archive_download_usage_bytes CHECK (accounted_bytes >= 0)",
      "expression": "(accounted_bytes >= 0)",
      "kind": "check",
      "name": "ck_archive_download_usage_bytes"
    }
  ],
  "name": "archive_download_usage"
}
```

</details>
