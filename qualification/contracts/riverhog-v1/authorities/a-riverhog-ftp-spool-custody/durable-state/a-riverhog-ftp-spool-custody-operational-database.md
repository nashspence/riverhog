# a-riverhog-ftp-spool-custody: operational-database

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-ftp-spool-custody:a-riverhog-ftp-spool-custody-operational-database:20d65e4294 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-fccbb8e17c"></a>



| Field | Value |
|---|---|
| <a id="s-130d2b802b"></a>`dialect` | `"sqlite"` |
| <a id="s-916a6ae7f2"></a>`id` | `"operational-database"` |
| <a id="s-2d0f540bea"></a>`kind` | `"relational-schema"` |
| <a id="s-ec209454a2"></a>`unique_indexes` | `[]` |
| <a id="s-a80b3bf11e"></a>`user_version` | `3` |

<a id="s-3f18564fa3"></a>

### Table: `claims`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-aeebe7c5fd"></a>`ordinal` | `INTEGER` | no | `—` | {"primary_key":true} |
| <a id="s-0bef2975e8"></a>`claim_id` | `TEXT` | no | `—` | {"unique":true} |
| <a id="s-e8a539f047"></a>`manifest_json` | `TEXT` | no | `—` | — |
| <a id="s-ee11d22ff5"></a>`claim_bytes` | `INTEGER` | no | `—` | {"checks":["(claim_bytes >= 0)"]} |

<a id="s-25b73bc528"></a>

### Table: `adapter_state`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-5a11255c05"></a>`key` | `TEXT` | yes | `—` | {"primary_key":true} |
| <a id="s-fafd99244a"></a>`value` | `TEXT` | no | `—` | — |

<a id="s-8bce6b4ef6"></a>

### Table: `completion_events`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1db4db2c8b"></a>`event_id` | `TEXT` | yes | `—` | {"primary_key":true} |
| <a id="s-ab175f668b"></a>`claim_id` | `TEXT` | no | `—` | — |

<a id="s-38849a284e"></a>

### Table: `completion_failures`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-15f4e086e3"></a>`ordinal` | `INTEGER` | no | `—` | {"primary_key":true} |
| <a id="s-81a59b74f1"></a>`failure_id` | `TEXT` | no | `—` | {"unique":true} |
| <a id="s-fdbb5e2173"></a>`generation` | `TEXT` | no | `—` | — |
| <a id="s-60408db64f"></a>`record_offset` | `INTEGER` | no | `—` | {"checks":["(record_offset >= 0)"]} |
| <a id="s-2036eaba00"></a>`raw` | `BLOB` | no | `—` | — |
| <a id="s-9b941b0847"></a>`reason` | `TEXT` | no | `—` | — |
| <a id="s-6c187171b3"></a>`retryable` | `INTEGER` | no | `—` | {"checks":["(retryable IN (0, 1))"]} |
| <a id="s-7a093a3457"></a>`attempts` | `INTEGER` | no | `0` | {"checks":["(attempts >= 0)"]} |

<a id="s-7737955f07"></a>

### Table: `lifecycle_events`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-eee332fd4c"></a>`sequence` | `INTEGER` | no | `—` | {"primary_key":true} |
| <a id="s-e770fc8844"></a>`event_id` | `TEXT` | no | `—` | {"unique":true} |
| <a id="s-b0ea76facc"></a>`event_json` | `TEXT` | no | `—` | — |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-ftp-spool-custody-durable-state-identity.md)

## Governing policies

- <a id="pa-7824ec2903"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-ftp-spool-custody](../../../evidence/sources/authorities.md#src-97232522a5) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/state\_contract.py::ftp\_custody\_state\_contract](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/state_contract.py)

### Machine authority

- `/external_contract/durable_state/owners/8/structure/units/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 03ebfe310c63a0d0e1f4e8ea12ce0b66b455dfd790c6fed5bf499bbdf1e73518 -->

```json
{
  "dialect": "sqlite",
  "id": "operational-database",
  "kind": "relational-schema",
  "tables": [
    {
      "columns": [
        {
          "definition": "ordinal INTEGER PRIMARY KEY",
          "name": "ordinal",
          "nullable": false,
          "primary_key": true,
          "type": "INTEGER"
        },
        {
          "definition": "claim_id TEXT NOT NULL UNIQUE",
          "name": "claim_id",
          "nullable": false,
          "type": "TEXT",
          "unique": true
        },
        {
          "definition": "manifest_json TEXT NOT NULL",
          "name": "manifest_json",
          "nullable": false,
          "type": "TEXT"
        },
        {
          "checks": [
            "(claim_bytes >= 0)"
          ],
          "definition": "claim_bytes INTEGER NOT NULL CHECK (claim_bytes >= 0)",
          "name": "claim_bytes",
          "nullable": false,
          "type": "INTEGER"
        }
      ],
      "constraints": [],
      "name": "claims"
    },
    {
      "columns": [
        {
          "definition": "key TEXT PRIMARY KEY",
          "name": "key",
          "nullable": true,
          "primary_key": true,
          "type": "TEXT"
        },
        {
          "definition": "value TEXT NOT NULL",
          "name": "value",
          "nullable": false,
          "type": "TEXT"
        }
      ],
      "constraints": [],
      "name": "adapter_state"
    },
    {
      "columns": [
        {
          "definition": "event_id TEXT PRIMARY KEY",
          "name": "event_id",
          "nullable": true,
          "primary_key": true,
          "type": "TEXT"
        },
        {
          "definition": "claim_id TEXT NOT NULL",
          "name": "claim_id",
          "nullable": false,
          "type": "TEXT"
        }
      ],
      "constraints": [],
      "name": "completion_events"
    },
    {
      "columns": [
        {
          "definition": "ordinal INTEGER PRIMARY KEY",
          "name": "ordinal",
          "nullable": false,
          "primary_key": true,
          "type": "INTEGER"
        },
        {
          "definition": "failure_id TEXT NOT NULL UNIQUE",
          "name": "failure_id",
          "nullable": false,
          "type": "TEXT",
          "unique": true
        },
        {
          "definition": "generation TEXT NOT NULL",
          "name": "generation",
          "nullable": false,
          "type": "TEXT"
        },
        {
          "checks": [
            "(record_offset >= 0)"
          ],
          "definition": "record_offset INTEGER NOT NULL CHECK (record_offset >= 0)",
          "name": "record_offset",
          "nullable": false,
          "type": "INTEGER"
        },
        {
          "definition": "raw BLOB NOT NULL",
          "name": "raw",
          "nullable": false,
          "type": "BLOB"
        },
        {
          "definition": "reason TEXT NOT NULL",
          "name": "reason",
          "nullable": false,
          "type": "TEXT"
        },
        {
          "checks": [
            "(retryable IN (0, 1))"
          ],
          "definition": "retryable INTEGER NOT NULL CHECK (retryable IN (0, 1))",
          "name": "retryable",
          "nullable": false,
          "type": "INTEGER"
        },
        {
          "checks": [
            "(attempts >= 0)"
          ],
          "default": "0",
          "definition": "attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0)",
          "name": "attempts",
          "nullable": false,
          "type": "INTEGER"
        }
      ],
      "constraints": [],
      "name": "completion_failures"
    },
    {
      "columns": [
        {
          "definition": "sequence INTEGER PRIMARY KEY AUTOINCREMENT",
          "name": "sequence",
          "nullable": false,
          "primary_key": true,
          "type": "INTEGER"
        },
        {
          "definition": "event_id TEXT NOT NULL UNIQUE",
          "name": "event_id",
          "nullable": false,
          "type": "TEXT",
          "unique": true
        },
        {
          "definition": "event_json TEXT NOT NULL",
          "name": "event_json",
          "nullable": false,
          "type": "TEXT"
        }
      ],
      "constraints": [],
      "name": "lifecycle_events"
    }
  ],
  "unique_indexes": [],
  "user_version": 3
}
```

</details>
