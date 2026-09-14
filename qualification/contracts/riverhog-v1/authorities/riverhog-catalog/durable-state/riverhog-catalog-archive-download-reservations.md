# riverhog-catalog: archive_download_reservations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-archive-download-reservations:893ea9db45 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-bb9d642cad"></a>
- Table: `archive_download_reservations`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-a4f3e61862"></a>`id` | `VARCHAR` | no | `—` | — |
| <a id="s-6bd793f5b6"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-5dadf4bbd2"></a>`month_started_at` | `VARCHAR` | no | `—` | — |
| <a id="s-e1af0577c4"></a>`reserved_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-aa4e0b66fe"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-d4f087bf52"></a>`expires_at` | `VARCHAR` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-edc54df050"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-15fd828ce9"></a>`foreign-key` | `—` | `FOREIGN KEY(store) REFERENCES archive_download_usage (store) ON DELETE CASCADE` |
| <a id="s-26656eb7d8"></a>`check` | `ck_archive_download_reservations_bytes` | `CONSTRAINT ck_archive_download_reservations_bytes CHECK (reserved_bytes >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-5203ed786c"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/17`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1815a13fff34c8128cba0b78b71ad6755ec1e42298af2d1866aed50de1615156 -->

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
        "store"
      ],
      "definition": "FOREIGN KEY(store) REFERENCES archive_download_usage (store) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "store"
        ],
        "table": "archive_download_usage"
      }
    },
    {
      "definition": "CONSTRAINT ck_archive_download_reservations_bytes CHECK (reserved_bytes >= 0)",
      "expression": "(reserved_bytes >= 0)",
      "kind": "check",
      "name": "ck_archive_download_reservations_bytes"
    }
  ],
  "name": "archive_download_reservations"
}
```
