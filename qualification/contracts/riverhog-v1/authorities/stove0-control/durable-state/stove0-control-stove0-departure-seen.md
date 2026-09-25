# stove0-control: stove0_departure_seen

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-departure-seen:9829f7b937 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-6090d5937b"></a>

### Table: `stove0_departure_seen`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-d883d07fe8"></a>`policy_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-ec0b255c10"></a>`generation` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-60a465a091"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-56e0172cdd"></a>`revision` | `VARCHAR(19)` | no | `—` | — |
| <a id="s-43d89b68c6"></a>`operation` | `VARCHAR(9)` | no | `—` | — |
| <a id="s-3f74a75dee"></a>`authority_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-b226e0eb2e"></a>`matched` | `BOOLEAN` | no | `—` | — |
| <a id="s-ec5964f7bb"></a>`document_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-37b65a83e3"></a>`document_json` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-d37e21b8ba"></a>`primary-key` | `—` | `PRIMARY KEY (policy_id, generation, collection_id)` |
| <a id="s-1a9e4c0a4a"></a>`check` | `ck_stove0_departure_seen_collection` | `CONSTRAINT ck_stove0_departure_seen_collection CHECK (collection_id >= 1)` |
| <a id="s-c4b70ead99"></a>`check` | `ck_stove0_departure_seen_operation` | `CONSTRAINT ck_stove0_departure_seen_operation CHECK (operation IN ('upsert','departure'))` |
| <a id="s-db1184188c"></a>`check` | `ck_stove0_departure_seen_document` | `CONSTRAINT ck_stove0_departure_seen_document CHECK (document_bytes IS NULL AND document_json IS NULL OR document_bytes >= 0 AND document_json IS NOT NULL)` |
| <a id="s-589e9a73ee"></a>`check` | `ck_stove0_departure_seen_generation_hex` | `CONSTRAINT ck_stove0_departure_seen_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-d55869541b"></a>`check` | `ck_stove0_departure_seen_authority_sha256_hex` | `CONSTRAINT ck_stove0_departure_seen_authority_sha256_hex CHECK (length(authority_sha256) = 64 AND lower(authority_sha256) = authority_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authority_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-b36a5f0d28"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/5/structure/tables/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e0cf1891656f10ac55aaba5d0e3c665cc797123aa769ba1151d1516a1c9f13b -->

```json
{
  "columns": [
    {
      "definition": "policy_id VARCHAR(160) NOT NULL",
      "name": "policy_id",
      "nullable": false,
      "type": "VARCHAR(160)"
    },
    {
      "definition": "generation VARCHAR(64) NOT NULL",
      "name": "generation",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "collection_id BIGINT NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "revision VARCHAR(19) NOT NULL",
      "name": "revision",
      "nullable": false,
      "type": "VARCHAR(19)"
    },
    {
      "definition": "operation VARCHAR(9) NOT NULL",
      "name": "operation",
      "nullable": false,
      "type": "VARCHAR(9)"
    },
    {
      "definition": "authority_sha256 VARCHAR(64) NOT NULL",
      "name": "authority_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "matched BOOLEAN NOT NULL",
      "name": "matched",
      "nullable": false,
      "type": "BOOLEAN"
    },
    {
      "definition": "document_bytes BIGINT",
      "name": "document_bytes",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "document_json TEXT",
      "name": "document_json",
      "nullable": true,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "policy_id",
        "generation",
        "collection_id"
      ],
      "definition": "PRIMARY KEY (policy_id, generation, collection_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_seen_collection CHECK (collection_id >= 1)",
      "expression": "(collection_id >= 1)",
      "kind": "check",
      "name": "ck_stove0_departure_seen_collection"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_seen_operation CHECK (operation IN ('upsert','departure'))",
      "expression": "(operation IN ('upsert','departure'))",
      "kind": "check",
      "name": "ck_stove0_departure_seen_operation"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_seen_document CHECK (document_bytes IS NULL AND document_json IS NULL OR document_bytes >= 0 AND document_json IS NOT NULL)",
      "expression": "(document_bytes IS NULL AND document_json IS NULL OR document_bytes >= 0 AND document_json IS NOT NULL)",
      "kind": "check",
      "name": "ck_stove0_departure_seen_document"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_seen_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_departure_seen_generation_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_seen_authority_sha256_hex CHECK (length(authority_sha256) = 64 AND lower(authority_sha256) = authority_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authority_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(authority_sha256) = 64 AND lower(authority_sha256) = authority_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authority_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_departure_seen_authority_sha256_hex"
    }
  ],
  "name": "stove0_departure_seen"
}
```

</details>
