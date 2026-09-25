# a-riverhog-minisign-witness-ledger: statements

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-minisign-witness-ledger:a-riverhog-minisign-witness-ledger-statements:11b9278461 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-minisign-witness-ledger](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-8896d26a74"></a>

### Table: `statements`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-f90539a670"></a>`digest` | `TEXT` | no | `—` | — |
| <a id="s-e734579d57"></a>`payload` | `BLOB` | no | `—` | — |
| <a id="s-1bd6cdb6ee"></a>`state` | `TEXT` | no | `—` | — |
| <a id="s-f93b65b455"></a>`attempts` | `INTEGER` | no | `—` | — |
| <a id="s-1b5bd822ff"></a>`due` | `INTEGER` | yes | `—` | — |
| <a id="s-be6b6a135e"></a>`error` | `TEXT` | yes | `—` | — |
| <a id="s-e0092d770e"></a>`signature` | `BLOB` | yes | `—` | — |
| <a id="s-a16bb34205"></a>`key_identity` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-4ab1932ad4"></a>`primary-key` | `—` | `PRIMARY KEY (digest)` |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-minisign-witness-ledger-durable-state-identity.md)

## Governing policies

- <a id="pa-090a7a673d"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-minisign-witness-ledger](../../../evidence/sources/authorities.md#src-d43dd7e522) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a\_riverhog\_minisign\_witness/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a_riverhog_minisign_witness/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/2`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4ab6ae33e062117c0705445c96980bc10a1d177723d6ac44602b6c67913676e3 -->

```json
{
  "columns": [
    {
      "definition": "digest TEXT NOT NULL",
      "name": "digest",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "payload BLOB NOT NULL",
      "name": "payload",
      "nullable": false,
      "type": "BLOB"
    },
    {
      "definition": "state TEXT NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "attempts INTEGER NOT NULL",
      "name": "attempts",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "due INTEGER",
      "name": "due",
      "nullable": true,
      "type": "INTEGER"
    },
    {
      "definition": "error TEXT",
      "name": "error",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "signature BLOB",
      "name": "signature",
      "nullable": true,
      "type": "BLOB"
    },
    {
      "definition": "key_identity TEXT",
      "name": "key_identity",
      "nullable": true,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "digest"
      ],
      "definition": "PRIMARY KEY (digest)",
      "kind": "primary-key"
    }
  ],
  "name": "statements"
}
```

</details>
