# a-riverhog-cli-local: desired_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-cli-local:a-riverhog-cli-local-desired-files:252e6fa55c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli-local](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-79b2dc4314"></a>

### Table: `desired_files`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-a5e14d88cb"></a>`collection_id` | `INTEGER` | no | `—` | — |
| <a id="s-e2b3393b96"></a>`path` | `TEXT` | no | `—` | — |
| <a id="s-466b135f1b"></a>`bytes` | `INTEGER` | no | `—` | — |
| <a id="s-32fe7b15a7"></a>`sha256` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-0ad4b16f0b"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, path)` |
| <a id="s-b307693025"></a>`check` | `ck_desired_files_bytes` | `CONSTRAINT ck_desired_files_bytes CHECK (bytes >= 0)` |
| <a id="s-318d0f94b1"></a>`check` | `ck_desired_files_sha256` | `CONSTRAINT ck_desired_files_sha256 CHECK (length(sha256) = 64 AND sha256 = lower(sha256) AND sha256 NOT GLOB '*[^0-9a-f]*')` |
| <a id="s-08965758bf"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES desired_collections (collection_id) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-cli-local-durable-state-identity.md)

## Governing policies

- <a id="pa-e5b7da6661"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-cli-local](../../../evidence/sources/authorities.md#src-196019bfcc) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/1/structure/tables/4`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b177ea9c21144365f71894c9a589ab01aeeff4f86ae290b2a2502b893633842a -->

```json
{
  "columns": [
    {
      "definition": "collection_id INTEGER NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "path TEXT NOT NULL",
      "name": "path",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "bytes INTEGER NOT NULL",
      "name": "bytes",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "sha256 TEXT NOT NULL",
      "name": "sha256",
      "nullable": false,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "path"
      ],
      "definition": "PRIMARY KEY (collection_id, path)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_desired_files_bytes CHECK (bytes >= 0)",
      "expression": "(bytes >= 0)",
      "kind": "check",
      "name": "ck_desired_files_bytes"
    },
    {
      "definition": "CONSTRAINT ck_desired_files_sha256 CHECK (length(sha256) = 64 AND sha256 = lower(sha256) AND sha256 NOT GLOB '*[^0-9a-f]*')",
      "expression": "(length(sha256) = 64 AND sha256 = lower(sha256) AND sha256 NOT GLOB '*[^0-9a-f]*')",
      "kind": "check",
      "name": "ck_desired_files_sha256"
    },
    {
      "columns": [
        "collection_id"
      ],
      "definition": "FOREIGN KEY(collection_id) REFERENCES desired_collections (collection_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id"
        ],
        "table": "desired_collections"
      }
    }
  ],
  "name": "desired_files"
}
```

</details>
