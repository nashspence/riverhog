# riverhog-catalog: archive_download_reservations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-archive-download-reservations:cdb8b96159 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-a2879b730f"></a>

### Table: `archive_download_reservations`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-bd4666ca28"></a>`id` | `VARCHAR` | no | `—` | — |
| <a id="s-bfefcfb058"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-10e4b0d4d7"></a>`month_started_at` | `VARCHAR` | no | `—` | — |
| <a id="s-9ae5495748"></a>`reserved_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-1ef5d906e5"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-249afa008b"></a>`expires_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-d780583b4c"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-3af3c2b24c"></a>`foreign-key` | `—` | `FOREIGN KEY(store) REFERENCES archive_download_usage (store) ON DELETE CASCADE` |
| <a id="s-3438fcb898"></a>`check` | `ck_archive_download_reservations_bytes` | `CONSTRAINT ck_archive_download_reservations_bytes CHECK (reserved_bytes >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-c12f9dd57f"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/18`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
