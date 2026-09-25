# a-riverhog-minisign-witness-ledger: progress

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-minisign-witness-ledger:a-riverhog-minisign-witness-ledger-progress:823fd527dc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-minisign-witness-ledger](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-e58bef7e49"></a>

### Table: `progress`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-14cfe6c26b"></a>`id` | `INTEGER` | no | `—` | — |
| <a id="s-3c62a5ea32"></a>`generation` | `INTEGER` | no | `—` | — |
| <a id="s-0fcbee6a32"></a>`serial` | `INTEGER` | no | `—` | — |
| <a id="s-9d801259b9"></a>`phase` | `TEXT` | no | `—` | — |
| <a id="s-d2ca25b581"></a>`source_identity` | `TEXT` | yes | `—` | — |
| <a id="s-a7549c86bf"></a>`authorization_view_identity` | `TEXT` | yes | `—` | — |
| <a id="s-863698b0d2"></a>`cursor` | `TEXT` | yes | `—` | — |
| <a id="s-9e18cd2ef1"></a>`through_revision` | `TEXT` | no | `—` | — |
| <a id="s-2fe5dbb4b6"></a>`last_collection_id` | `INTEGER` | yes | `—` | — |
| <a id="s-004899d824"></a>`reset_reason` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-c6d0508442"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-minisign-witness-ledger-durable-state-identity.md)

## Governing policies

- <a id="pa-c5b6bb28fa"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-minisign-witness-ledger](../../../evidence/sources/authorities.md#src-d43dd7e522) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a\_riverhog\_minisign\_witness/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a_riverhog_minisign_witness/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/1`

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
