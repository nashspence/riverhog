# riverhog-catalog: app_key_access_grants

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-app-key-access-grants:c3918d78a1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-bb9d642cad"></a>

### Table: `app_key_access_grants`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-a4f3e61862"></a>`key_id` | `VARCHAR` | no | `—` | — |
| <a id="s-6bd793f5b6"></a>`permission` | `VARCHAR` | no | `—` | — |
| <a id="s-5dadf4bbd2"></a>`resource` | `VARCHAR` | no | `—` | — |
| <a id="s-e1af0577c4"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-aa4e0b66fe"></a>`search_text` | `VARCHAR` | no | `—` | {"generated":"GENERATED ALWAYS AS (lower(permission \|\| ' ' \|\| resource)) STORED"} |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-edc54df050"></a>`primary-key` | `—` | `PRIMARY KEY (key_id, permission, resource)` |
| <a id="s-15fd828ce9"></a>`foreign-key` | `—` | `FOREIGN KEY(key_id) REFERENCES app_keys (id) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-81330c6145"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/17`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6723323592d19871109d302087a115431d55013f0e8e439e41968811cd143b18 -->

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
      "definition": "permission VARCHAR NOT NULL",
      "name": "permission",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "resource VARCHAR NOT NULL",
      "name": "resource",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "search_text VARCHAR GENERATED ALWAYS AS (lower(permission || ' ' || resource)) STORED NOT NULL",
      "generated": "GENERATED ALWAYS AS (lower(permission || ' ' || resource)) STORED",
      "name": "search_text",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "key_id",
        "permission",
        "resource"
      ],
      "definition": "PRIMARY KEY (key_id, permission, resource)",
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
    }
  ],
  "name": "app_key_access_grants"
}
```

</details>
