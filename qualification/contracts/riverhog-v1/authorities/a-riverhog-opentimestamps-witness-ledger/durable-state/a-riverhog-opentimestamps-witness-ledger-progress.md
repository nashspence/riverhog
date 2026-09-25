# a-riverhog-opentimestamps-witness-ledger: progress

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-opentimestamps-witness-ledger:a-riverhog-opentimestamps-witness-ledger-progress:8f53b52212 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness-ledger](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-1982d018ff"></a>

### Table: `progress`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-4342ec0d6f"></a>`id` | `INTEGER` | no | `—` | — |
| <a id="s-7744cf4205"></a>`generation` | `INTEGER` | no | `—` | — |
| <a id="s-907a253d6d"></a>`serial` | `INTEGER` | no | `—` | — |
| <a id="s-fd136fa2a9"></a>`phase` | `TEXT` | no | `—` | — |
| <a id="s-a757e363e4"></a>`source_identity` | `TEXT` | yes | `—` | — |
| <a id="s-77293fa373"></a>`authorization_view_identity` | `TEXT` | yes | `—` | — |
| <a id="s-be8c51d627"></a>`cursor` | `TEXT` | yes | `—` | — |
| <a id="s-4fc70c0351"></a>`through_revision` | `TEXT` | no | `—` | — |
| <a id="s-d2e64cced1"></a>`last_collection_id` | `INTEGER` | yes | `—` | — |
| <a id="s-8f04691ce1"></a>`reset_reason` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-93c6dcf0b6"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-opentimestamps-witness-ledger-durable-state-identity.md)

## Governing policies

- <a id="pa-f56e3fce42"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-opentimestamps-witness-ledger](../../../evidence/sources/authorities.md#src-31e2884ee3) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/4/structure/tables/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c85b5d75b9d02b7079e4f0b0fc39fa46baa7d709a5b8021c8c26a9c5968d558a -->

```json
{
  "columns": [
    {
      "definition": "id INTEGER NOT NULL",
      "name": "id",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "generation INTEGER NOT NULL",
      "name": "generation",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "serial INTEGER NOT NULL",
      "name": "serial",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "phase TEXT NOT NULL",
      "name": "phase",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "source_identity TEXT",
      "name": "source_identity",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "authorization_view_identity TEXT",
      "name": "authorization_view_identity",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "cursor TEXT",
      "name": "cursor",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "through_revision TEXT NOT NULL",
      "name": "through_revision",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "last_collection_id INTEGER",
      "name": "last_collection_id",
      "nullable": true,
      "type": "INTEGER"
    },
    {
      "definition": "reset_reason TEXT",
      "name": "reset_reason",
      "nullable": true,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "id"
      ],
      "definition": "PRIMARY KEY (id)",
      "kind": "primary-key"
    }
  ],
  "name": "progress"
}
```

</details>
