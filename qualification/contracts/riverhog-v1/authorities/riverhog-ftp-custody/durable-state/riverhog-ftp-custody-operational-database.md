# riverhog-ftp-custody: operational-database

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-ftp-custody:riverhog-ftp-custody-operational-database:6528f403a4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-992b75bfec"></a>

| Field | Shape |
|---|---|
| <a id="s-9c6dbe1166"></a>`dialect` | "sqlite" |
| <a id="s-a30d878c25"></a>`id` | "operational-database" |
| <a id="s-41e1afd5b1"></a>`kind` | "relational-schema" |
| <a id="s-79aa9488de"></a>`tables` | items=additional keys=`columns`, `constraints`, `name` \| additional keys=`columns`, `constraints`, `name` \| additional keys=`columns`, `constraints`, `name` \| additional keys=`columns`, `constraints`, `name` |
| <a id="s-bc4deff50d"></a>`unique_indexes` | [] |
| <a id="s-0d88293dad"></a>`user_version` | 2 |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-ftp-custody-durable-state-identity.md)

## Governing policies

- <a id="pa-5d85c95598"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-ftp-custody](../../../evidence/sources.md#src-54f88a3a47) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/state_contract.py`

### Machine authority

- `/external_contract/durable_state/owners/6/structure/units/0`

### Exact owned JSON

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
