# a-riverhog-opentimestamps-witness-ledger: statements

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-opentimestamps-witness-ledger:a-riverhog-opentimestamps-witness-ledger-statements:9bc3d44bf4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness-ledger](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-3d5c1cecb3"></a>

### Table: `statements`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-154782d54d"></a>`digest` | `TEXT` | no | `—` | — |
| <a id="s-4d2e467a23"></a>`payload` | `BLOB` | no | `—` | — |
| <a id="s-110ebb6ba4"></a>`job_state` | `BLOB` | no | `—` | — |
| <a id="s-5d8e3d6c2c"></a>`job_version` | `INTEGER` | no | `—` | — |
| <a id="s-f538c551fd"></a>`due` | `INTEGER` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-f4f7620f57"></a>`primary-key` | `—` | `PRIMARY KEY (digest)` |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-opentimestamps-witness-ledger-durable-state-identity.md)

## Governing policies

- <a id="pa-cfb4b2ea2d"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-opentimestamps-witness-ledger](../../../evidence/sources/authorities.md#src-31e2884ee3) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/4/structure/tables/3`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30b797d3228dd054ee9508a7a75676da0ecb88482032299e1770dd32037cda04 -->

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
      "definition": "job_state BLOB NOT NULL",
      "name": "job_state",
      "nullable": false,
      "type": "BLOB"
    },
    {
      "definition": "job_version INTEGER NOT NULL",
      "name": "job_version",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "due INTEGER",
      "name": "due",
      "nullable": true,
      "type": "INTEGER"
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
