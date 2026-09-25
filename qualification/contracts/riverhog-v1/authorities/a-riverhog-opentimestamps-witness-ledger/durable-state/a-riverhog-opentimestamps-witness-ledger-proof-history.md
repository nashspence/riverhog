# a-riverhog-opentimestamps-witness-ledger: proof_history

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-opentimestamps-witness-ledger:a-riverhog-opentimestamps-witness-ledger-1564b355ce:49f20e5bfd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness-ledger](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-dd804d3f81"></a>

### Table: `proof_history`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-35e81c8718"></a>`digest` | `TEXT` | no | `—` | — |
| <a id="s-245d35832a"></a>`revision` | `INTEGER` | no | `—` | — |
| <a id="s-d545428730"></a>`proof` | `BLOB` | no | `—` | — |
| <a id="s-5af5d654b2"></a>`proof_digest` | `TEXT` | no | `—` | — |
| <a id="s-e0e7779d0d"></a>`recorded_at` | `INTEGER` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-8d171c5618"></a>`primary-key` | `—` | `PRIMARY KEY (digest, revision)` |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-opentimestamps-witness-ledger-durable-state-identity.md)

## Governing policies

- <a id="pa-835a5c656d"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-opentimestamps-witness-ledger](../../../evidence/sources/authorities.md#src-31e2884ee3) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/4/structure/tables/2`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a6a5ba83a687c2e269a5f11f70eb9b95685166aee4d39193816990c5ec178498 -->

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
      "definition": "revision INTEGER NOT NULL",
      "name": "revision",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "proof BLOB NOT NULL",
      "name": "proof",
      "nullable": false,
      "type": "BLOB"
    },
    {
      "definition": "proof_digest TEXT NOT NULL",
      "name": "proof_digest",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "recorded_at INTEGER NOT NULL",
      "name": "recorded_at",
      "nullable": false,
      "type": "INTEGER"
    }
  ],
  "constraints": [
    {
      "columns": [
        "digest",
        "revision"
      ],
      "definition": "PRIMARY KEY (digest, revision)",
      "kind": "primary-key"
    }
  ],
  "name": "proof_history"
}
```

</details>
