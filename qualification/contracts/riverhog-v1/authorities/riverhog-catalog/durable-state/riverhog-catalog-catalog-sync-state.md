# riverhog-catalog: catalog_sync_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-catalog-sync-state:6e68fe1db3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-08ed02092e"></a>

### Table: `catalog_sync_state`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-a977a0f315"></a>`singleton` | `SERIAL` | no | `—` | — |
| <a id="s-aaa5b757fb"></a>`source_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-445eb048f6"></a>`committed_revision` | `BIGINT` | no | `0` | — |
| <a id="s-21ccb15417"></a>`retained_revision` | `BIGINT` | no | `0` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-382bfdb1d4"></a>`primary-key` | `—` | `PRIMARY KEY (singleton)` |
| <a id="s-50e2b09328"></a>`check` | `ck_catalog_sync_state_singleton` | `CONSTRAINT ck_catalog_sync_state_singleton CHECK (singleton = 1)` |
| <a id="s-70a38d8476"></a>`check` | `ck_catalog_sync_state_committed_revision` | `CONSTRAINT ck_catalog_sync_state_committed_revision CHECK (committed_revision >= 0)` |
| <a id="s-87abe469e7"></a>`check` | `ck_catalog_sync_state_retained_revision` | `CONSTRAINT ck_catalog_sync_state_retained_revision CHECK (retained_revision >= 0 AND retained_revision <= committed_revision)` |
| <a id="s-239c48418a"></a>`check` | `ck_catalog_sync_state_source_identity_hex` | `CONSTRAINT ck_catalog_sync_state_source_identity_hex CHECK (length(source_identity) = 64 AND lower(source_identity) = source_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-eb6bbbf01b"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/3`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5e8ba505ef96333c0b9e426af9e2031000772d9b02b6230eaa1e95443e1a75a9 -->

```json
{
  "columns": [
    {
      "definition": "singleton SERIAL NOT NULL",
      "name": "singleton",
      "nullable": false,
      "type": "SERIAL"
    },
    {
      "definition": "source_identity VARCHAR(64) NOT NULL",
      "name": "source_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "default": "0",
      "definition": "committed_revision BIGINT DEFAULT 0 NOT NULL",
      "name": "committed_revision",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "default": "0",
      "definition": "retained_revision BIGINT DEFAULT 0 NOT NULL",
      "name": "retained_revision",
      "nullable": false,
      "type": "BIGINT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "singleton"
      ],
      "definition": "PRIMARY KEY (singleton)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_catalog_sync_state_singleton CHECK (singleton = 1)",
      "expression": "(singleton = 1)",
      "kind": "check",
      "name": "ck_catalog_sync_state_singleton"
    },
    {
      "definition": "CONSTRAINT ck_catalog_sync_state_committed_revision CHECK (committed_revision >= 0)",
      "expression": "(committed_revision >= 0)",
      "kind": "check",
      "name": "ck_catalog_sync_state_committed_revision"
    },
    {
      "definition": "CONSTRAINT ck_catalog_sync_state_retained_revision CHECK (retained_revision >= 0 AND retained_revision <= committed_revision)",
      "expression": "(retained_revision >= 0 AND retained_revision <= committed_revision)",
      "kind": "check",
      "name": "ck_catalog_sync_state_retained_revision"
    },
    {
      "definition": "CONSTRAINT ck_catalog_sync_state_source_identity_hex CHECK (length(source_identity) = 64 AND lower(source_identity) = source_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(source_identity) = 64 AND lower(source_identity) = source_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_catalog_sync_state_source_identity_hex"
    }
  ],
  "name": "catalog_sync_state"
}
```

</details>
