# a-riverhog-minisign-witness-ledger: observations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-minisign-witness-ledger:a-riverhog-minisign-witness-ledger-observations:b58fdfba86 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-minisign-witness-ledger](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-9fcc47afa8"></a>

### Table: `observations`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-0eb2e1316e"></a>`generation` | `INTEGER` | no | `—` | — |
| <a id="s-e994025205"></a>`collection_id` | `INTEGER` | no | `—` | — |
| <a id="s-942b0af2fd"></a>`revision` | `TEXT` | no | `—` | — |
| <a id="s-059199b5d0"></a>`document` | `BLOB` | no | `—` | — |
| <a id="s-27a3ad2ff9"></a>`statement_digest` | `TEXT` | yes | `—` | — |
| <a id="s-02c49023f8"></a>`departed` | `INTEGER` | no | `—` | — |
| <a id="s-58b23f9aa7"></a>`departure_cause` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-255e4b2132"></a>`primary-key` | `—` | `PRIMARY KEY (generation, collection_id)` |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-minisign-witness-ledger-durable-state-identity.md)

## Governing policies

- <a id="pa-2f4bf73deb"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-minisign-witness-ledger](../../../evidence/sources/authorities.md#src-d43dd7e522) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a\_riverhog\_minisign\_witness/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a_riverhog_minisign_witness/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/0`

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
