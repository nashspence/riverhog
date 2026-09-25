# a-riverhog-opentimestamps-witness-ledger: observations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-opentimestamps-witness-ledger:a-riverhog-opentimestamps-witness-ledger-c65adce348:74c1d593c9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness-ledger](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-636a85068a"></a>

### Table: `observations`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-0a6bcdef95"></a>`generation` | `INTEGER` | no | `—` | — |
| <a id="s-8705138789"></a>`collection_id` | `INTEGER` | no | `—` | — |
| <a id="s-d073e17d4a"></a>`revision` | `TEXT` | no | `—` | — |
| <a id="s-4913359baf"></a>`document` | `BLOB` | no | `—` | — |
| <a id="s-6c0fae9fcd"></a>`statement_digest` | `TEXT` | yes | `—` | — |
| <a id="s-fd1a3109f5"></a>`departed` | `INTEGER` | no | `—` | — |
| <a id="s-e224d926aa"></a>`departure_cause` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-2df0d92332"></a>`primary-key` | `—` | `PRIMARY KEY (generation, collection_id)` |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-opentimestamps-witness-ledger-durable-state-identity.md)

## Governing policies

- <a id="pa-9e18b49182"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-opentimestamps-witness-ledger](../../../evidence/sources/authorities.md#src-31e2884ee3) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/4/structure/tables/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3fb034e33252baf50a9f99748a72d521423b17ac8adbab723c41cf3a75eb54a6 -->

```json
{
  "columns": [
    {
      "definition": "generation INTEGER NOT NULL",
      "name": "generation",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "collection_id INTEGER NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "revision TEXT NOT NULL",
      "name": "revision",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "document BLOB NOT NULL",
      "name": "document",
      "nullable": false,
      "type": "BLOB"
    },
    {
      "definition": "statement_digest TEXT",
      "name": "statement_digest",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "departed INTEGER NOT NULL",
      "name": "departed",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "departure_cause TEXT",
      "name": "departure_cause",
      "nullable": true,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "generation",
        "collection_id"
      ],
      "definition": "PRIMARY KEY (generation, collection_id)",
      "kind": "primary-key"
    }
  ],
  "name": "observations"
}
```

</details>
