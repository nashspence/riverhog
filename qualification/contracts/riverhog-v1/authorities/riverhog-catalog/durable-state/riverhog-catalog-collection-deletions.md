# riverhog-catalog: collection_deletions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-deletions:79a632e711 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-f811bc04ba"></a>

### Table: `collection_deletions`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-07e2993f28"></a>`collection_id` | `BIGSERIAL` | no | `—` | — |
| <a id="s-06a852d00a"></a>`challenge` | `VARCHAR` | no | `—` | — |
| <a id="s-902e86644e"></a>`plan_json` | `TEXT` | no | `—` | — |
| <a id="s-641cebaa4c"></a>`started_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-60f0632d57"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-9320b19343"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/4`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5482665f9965330210e8f6c7c6932dc88dcdb65e69cf93eec3234cc39148775e -->

```json
{
  "columns": [
    {
      "definition": "collection_id BIGSERIAL NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGSERIAL"
    },
    {
      "definition": "challenge VARCHAR NOT NULL",
      "name": "challenge",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "plan_json TEXT NOT NULL",
      "name": "plan_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "started_at VARCHAR NOT NULL",
      "name": "started_at",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id"
      ],
      "definition": "PRIMARY KEY (collection_id)",
      "kind": "primary-key"
    }
  ],
  "name": "collection_deletions"
}
```

</details>
