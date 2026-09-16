# riverhog-catalog: collection_provenance_journals

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-provenance-journals:74bf39bcae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-3811e19ab2"></a>

### Table: `collection_provenance_journals`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-603e1ec3dc"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-27b968f672"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-cc8df404f2"></a>`bytes` | `BIGINT` | no | `—` | — |
| <a id="s-3d3eecca03"></a>`sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-015771d2cc"></a>`entries` | `BIGINT` | no | `—` | — |
| <a id="s-1db36a3f8f"></a>`agent_count` | `BIGINT` | no | `—` | — |
| <a id="s-571a8a09e3"></a>`entity_counts_json` | `TEXT` | no | `—` | — |
| <a id="s-c80f98a8ee"></a>`current_state_id` | `VARCHAR` | no | `—` | — |
| <a id="s-1da64bc09c"></a>`current_entry_id` | `VARCHAR` | no | `—` | — |
| <a id="s-34a1c08489"></a>`current_entry_json_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-a8de762bf9"></a>`current_path` | `VARCHAR` | no | `—` | — |
| <a id="s-12aff18420"></a>`current_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-a386f46988"></a>`current_sha256` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-cd362c9a0c"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id)` |
| <a id="s-06b97722b5"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-df5e0b99f0"></a>`check` | `ck_provenance_journals_bytes` | `CONSTRAINT ck_provenance_journals_bytes CHECK (bytes >= 0)` |
| <a id="s-5511f8445a"></a>`check` | `ck_provenance_journals_entries` | `CONSTRAINT ck_provenance_journals_entries CHECK (entries >= 0)` |
| <a id="s-77366b2e95"></a>`check` | `ck_provenance_journals_agent_count` | `CONSTRAINT ck_provenance_journals_agent_count CHECK (agent_count >= 0)` |
| <a id="s-a6b03442de"></a>`check` | `ck_provenance_journals_current_bytes` | `CONSTRAINT ck_provenance_journals_current_bytes CHECK (current_bytes >= 0)` |
| <a id="s-13ac8be7e6"></a>`check` | `ck_provenance_journals_sha256` | `CONSTRAINT ck_provenance_journals_sha256 CHECK (length(sha256) = 64)` |
| <a id="s-4da4891634"></a>`check` | `ck_collection_provenance_journals_sha256_hex` | `CONSTRAINT ck_collection_provenance_journals_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-ebb811eed6"></a>`check` | `ck_sha256_4b4f04aff752e0e1` | `CONSTRAINT ck_sha256_4b4f04aff752e0e1 CHECK (length(current_entry_json_sha256) = 64 AND lower(current_entry_json_sha256) = current_entry_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_entry_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-262b01a414"></a>`check` | `ck_collection_provenance_journals_current_sha256_hex` | `CONSTRAINT ck_collection_provenance_journals_current_sha256_hex CHECK (length(current_sha256) = 64 AND lower(current_sha256) = current_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-f91cbe3720"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/22`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1f5babf0a4f4682bbe8abff8e1700d86eb72a69bb5209ae8b3ece00b9428d40f -->

```json
{
  "columns": [
    {
      "definition": "collection_id BIGINT NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "journal_id VARCHAR NOT NULL",
      "name": "journal_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "bytes BIGINT NOT NULL",
      "name": "bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "sha256 VARCHAR(64) NOT NULL",
      "name": "sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "entries BIGINT NOT NULL",
      "name": "entries",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "agent_count BIGINT NOT NULL",
      "name": "agent_count",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "entity_counts_json TEXT NOT NULL",
      "name": "entity_counts_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "current_state_id VARCHAR NOT NULL",
      "name": "current_state_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "current_entry_id VARCHAR NOT NULL",
      "name": "current_entry_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "current_entry_json_sha256 VARCHAR(64) NOT NULL",
      "name": "current_entry_json_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "current_path VARCHAR NOT NULL",
      "name": "current_path",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "current_bytes BIGINT NOT NULL",
      "name": "current_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "current_sha256 VARCHAR(64) NOT NULL",
      "name": "current_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "journal_id"
      ],
      "definition": "PRIMARY KEY (collection_id, journal_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id"
      ],
      "definition": "FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "id"
        ],
        "table": "collections"
      }
    },
    {
      "definition": "CONSTRAINT ck_provenance_journals_bytes CHECK (bytes >= 0)",
      "expression": "(bytes >= 0)",
      "kind": "check",
      "name": "ck_provenance_journals_bytes"
    },
    {
      "definition": "CONSTRAINT ck_provenance_journals_entries CHECK (entries >= 0)",
      "expression": "(entries >= 0)",
      "kind": "check",
      "name": "ck_provenance_journals_entries"
    },
    {
      "definition": "CONSTRAINT ck_provenance_journals_agent_count CHECK (agent_count >= 0)",
      "expression": "(agent_count >= 0)",
      "kind": "check",
      "name": "ck_provenance_journals_agent_count"
    },
    {
      "definition": "CONSTRAINT ck_provenance_journals_current_bytes CHECK (current_bytes >= 0)",
      "expression": "(current_bytes >= 0)",
      "kind": "check",
      "name": "ck_provenance_journals_current_bytes"
    },
    {
      "definition": "CONSTRAINT ck_provenance_journals_sha256 CHECK (length(sha256) = 64)",
      "expression": "(length(sha256) = 64)",
      "kind": "check",
      "name": "ck_provenance_journals_sha256"
    },
    {
      "definition": "CONSTRAINT ck_collection_provenance_journals_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_provenance_journals_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_sha256_4b4f04aff752e0e1 CHECK (length(current_entry_json_sha256) = 64 AND lower(current_entry_json_sha256) = current_entry_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_entry_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(current_entry_json_sha256) = 64 AND lower(current_entry_json_sha256) = current_entry_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_entry_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_4b4f04aff752e0e1"
    },
    {
      "definition": "CONSTRAINT ck_collection_provenance_journals_current_sha256_hex CHECK (length(current_sha256) = 64 AND lower(current_sha256) = current_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(current_sha256) = 64 AND lower(current_sha256) = current_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_provenance_journals_current_sha256_hex"
    }
  ],
  "name": "collection_provenance_journals"
}
```

</details>
