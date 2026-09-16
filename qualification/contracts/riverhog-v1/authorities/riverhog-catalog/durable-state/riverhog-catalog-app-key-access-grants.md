# riverhog-catalog: app_key_access_grants

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-app-key-access-grants:c636cef3f7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-f43be24d3d"></a>

### Table: `app_key_access_grants`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-b86e579b0a"></a>`key_id` | `VARCHAR` | no | `—` | — |
| <a id="s-36749376fd"></a>`permission` | `VARCHAR` | no | `—` | — |
| <a id="s-47ee498a3d"></a>`resource` | `VARCHAR` | no | `—` | — |
| <a id="s-d6b73a2206"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-130a9f5caf"></a>`search_text` | `VARCHAR` | no | `—` | {"generated":"GENERATED ALWAYS AS (lower(permission \|\| ' ' \|\| resource)) STORED"} |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-92e3b4b53c"></a>`primary-key` | `—` | `PRIMARY KEY (key_id, permission, resource)` |
| <a id="s-a87ef7b851"></a>`foreign-key` | `—` | `FOREIGN KEY(key_id) REFERENCES app_keys (id) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-f421cc5d7b"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/16`

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
