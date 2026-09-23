# a-riverhog-ftp-spool-custody: operational-database

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-ftp-spool-custody:a-riverhog-ftp-spool-custody-operational-database:a78d412e76 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-992b75bfec"></a>



| Field | Value |
|---|---|
| <a id="s-9c6dbe1166"></a>`dialect` | `"sqlite"` |
| <a id="s-a30d878c25"></a>`id` | `"operational-database"` |
| <a id="s-41e1afd5b1"></a>`kind` | `"relational-schema"` |
| <a id="s-bc4deff50d"></a>`unique_indexes` | `[]` |
| <a id="s-0d88293dad"></a>`user_version` | `2` |

<a id="s-9d10de3bf6"></a>

### Table: `claims`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-c2d025d372"></a>`ordinal` | `INTEGER` | no | `—` | {"primary_key":true} |
| <a id="s-f05d73dc5b"></a>`claim_id` | `TEXT` | no | `—` | {"unique":true} |
| <a id="s-bc267481ba"></a>`manifest_json` | `TEXT` | no | `—` | — |
| <a id="s-a74c0b238b"></a>`claim_bytes` | `INTEGER` | no | `—` | {"checks":["(claim_bytes >= 0)"]} |

<a id="s-af955cbda3"></a>

### Table: `adapter_state`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-8e6cc5f51d"></a>`key` | `TEXT` | yes | `—` | {"primary_key":true} |
| <a id="s-3e385f1523"></a>`value` | `TEXT` | no | `—` | — |

<a id="s-2364ff26c0"></a>

### Table: `completion_events`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-cdbbd2f9e5"></a>`event_id` | `TEXT` | yes | `—` | {"primary_key":true} |
| <a id="s-9b5453ce77"></a>`claim_id` | `TEXT` | no | `—` | — |

<a id="s-e2331c1394"></a>

### Table: `completion_failures`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-7ffa6be99d"></a>`ordinal` | `INTEGER` | no | `—` | {"primary_key":true} |
| <a id="s-7eba67e027"></a>`failure_id` | `TEXT` | no | `—` | {"unique":true} |
| <a id="s-30c0c26ff5"></a>`generation` | `TEXT` | no | `—` | — |
| <a id="s-6a29c38391"></a>`record_offset` | `INTEGER` | no | `—` | {"checks":["(record_offset >= 0)"]} |
| <a id="s-edbc45528f"></a>`raw` | `BLOB` | no | `—` | — |
| <a id="s-5965aeadd4"></a>`reason` | `TEXT` | no | `—` | — |
| <a id="s-0b8258b646"></a>`retryable` | `INTEGER` | no | `—` | {"checks":["(retryable IN (0, 1))"]} |
| <a id="s-edc8fa5a3a"></a>`attempts` | `INTEGER` | no | `0` | {"checks":["(attempts >= 0)"]} |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-ftp-spool-custody-durable-state-identity.md)

## Governing policies

- <a id="pa-960ca824e6"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-ftp-spool-custody](../../../evidence/sources/authorities.md#src-97232522a5) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/state\_contract.py::ftp\_custody\_state\_contract](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/state_contract.py)

### Machine authority

- `/external_contract/durable_state/owners/6/structure/units/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1513b8c445d9c9243e439461f0647ee8110f5669ec5442e059c2181a0b938f15 -->

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
    }
  ],
  "unique_indexes": [],
  "user_version": 2
}
```

</details>
