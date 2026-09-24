# riverhog-catalog: retrieval_plans

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-plans:51592d3801 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-f43be24d3d"></a>

### Table: `retrieval_plans`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-b86e579b0a"></a>`id` | `VARCHAR` | no | `—` | — |
| <a id="s-36749376fd"></a>`principal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-47ee498a3d"></a>`initiated_by_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-d6b73a2206"></a>`idempotency_key` | `VARCHAR` | no | `—` | — |
| <a id="s-130a9f5caf"></a>`creation_identity_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-3ad1c99d8e"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-9d69a6a023"></a>`request_json` | `TEXT` | no | `—` | — |
| <a id="s-83a22533c6"></a>`lease_seconds` | `BIGINT` | no | `—` | — |
| <a id="s-24f7748d58"></a>`restore_policy` | `VARCHAR` | no | `—` | — |
| <a id="s-ba18dc97d4"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-d40d8ea029"></a>`ready_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-e0e6317db7"></a>`expires_at` | `VARCHAR` | no | `—` | — |
| <a id="s-3436492270"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-21799f3024"></a>`next_file_order` | `INTEGER` | no | `—` | — |
| <a id="s-ee45cde7b3"></a>`next_placement_sequence` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-8fff849204"></a>`object_count` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-1b3ce4c775"></a>`retrieval_bytes` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-1ebd2cd070"></a>`requires_restore` | `BOOLEAN` | no | `—` | — |
| <a id="s-780714a5fe"></a>`file_commitment_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-5cd547d94d"></a>`segment_commitment_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-fb92ee17b5"></a>`etag` | `VARCHAR(64)` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-92e3b4b53c"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-a87ef7b851"></a>`unique` | `uq_retrieval_plans_key_idempotency` | `CONSTRAINT uq_retrieval_plans_key_idempotency UNIQUE (principal_id, initiated_by_key_id, idempotency_key)` |
| <a id="s-711642855e"></a>`check` | `ck_retrieval_plans_state` | `CONSTRAINT ck_retrieval_plans_state CHECK (state IN ('planning','ready','consumed','expired','failed'))` |
| <a id="s-ef575f5074"></a>`check` | `ck_retrieval_plans_lease` | `CONSTRAINT ck_retrieval_plans_lease CHECK (lease_seconds > 0)` |
| <a id="s-892ef661e3"></a>`check` | `ck_retrieval_plans_restore_policy` | `CONSTRAINT ck_retrieval_plans_restore_policy CHECK (restore_policy IN ('allow','never'))` |
| <a id="s-13740d8ac3"></a>`check` | `ck_retrieval_plans_file_order` | `CONSTRAINT ck_retrieval_plans_file_order CHECK (next_file_order >= 0)` |
| <a id="s-9d75aa9f79"></a>`check` | `ck_retrieval_plans_creation_identity_sha256_hex` | `CONSTRAINT ck_retrieval_plans_creation_identity_sha256_hex CHECK (length(creation_identity_sha256) = 64 AND lower(creation_identity_sha256) = creation_identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(creation_identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-a55a612fe3"></a>`check` | `ck_retrieval_plans_file_commitment_sha256_hex` | `CONSTRAINT ck_retrieval_plans_file_commitment_sha256_hex CHECK (length(file_commitment_sha256) = 64 AND lower(file_commitment_sha256) = file_commitment_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(file_commitment_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-5d958a7f44"></a>`check` | `ck_retrieval_plans_segment_commitment_sha256_hex` | `CONSTRAINT ck_retrieval_plans_segment_commitment_sha256_hex CHECK (length(segment_commitment_sha256) = 64 AND lower(segment_commitment_sha256) = segment_commitment_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(segment_commitment_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-63c9e5e59d"></a>`check` | `ck_retrieval_plans_etag_hex` | `CONSTRAINT ck_retrieval_plans_etag_hex CHECK (etag IS NULL OR length(etag) = 64 AND lower(etag) = etag AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(etag, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-61da383841"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/16`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 99bd462bd38386f5a471419d0ded93c102698c8a1dfce90a919473ff19333085 -->

```json
{
  "columns": [
    {
      "definition": "id VARCHAR NOT NULL",
      "name": "id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "principal_id VARCHAR NOT NULL",
      "name": "principal_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "initiated_by_key_id VARCHAR",
      "name": "initiated_by_key_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "idempotency_key VARCHAR NOT NULL",
      "name": "idempotency_key",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "creation_identity_sha256 VARCHAR(64) NOT NULL",
      "name": "creation_identity_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "request_json TEXT NOT NULL",
      "name": "request_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "lease_seconds BIGINT NOT NULL",
      "name": "lease_seconds",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "restore_policy VARCHAR NOT NULL",
      "name": "restore_policy",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "ready_at VARCHAR",
      "name": "ready_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "expires_at VARCHAR NOT NULL",
      "name": "expires_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "failure TEXT",
      "name": "failure",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "next_file_order INTEGER NOT NULL",
      "name": "next_file_order",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "next_placement_sequence VARCHAR(64) NOT NULL",
      "name": "next_placement_sequence",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "object_count VARCHAR(64) NOT NULL",
      "name": "object_count",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "retrieval_bytes VARCHAR(64) NOT NULL",
      "name": "retrieval_bytes",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "requires_restore BOOLEAN NOT NULL",
      "name": "requires_restore",
      "nullable": false,
      "type": "BOOLEAN"
    },
    {
      "definition": "file_commitment_sha256 VARCHAR(64) NOT NULL",
      "name": "file_commitment_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "segment_commitment_sha256 VARCHAR(64) NOT NULL",
      "name": "segment_commitment_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "etag VARCHAR(64)",
      "name": "etag",
      "nullable": true,
      "type": "VARCHAR(64)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "id"
      ],
      "definition": "PRIMARY KEY (id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "principal_id",
        "initiated_by_key_id",
        "idempotency_key"
      ],
      "definition": "CONSTRAINT uq_retrieval_plans_key_idempotency UNIQUE (principal_id, initiated_by_key_id, idempotency_key)",
      "kind": "unique",
      "name": "uq_retrieval_plans_key_idempotency"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plans_state CHECK (state IN ('planning','ready','consumed','expired','failed'))",
      "expression": "(state IN ('planning','ready','consumed','expired','failed'))",
      "kind": "check",
      "name": "ck_retrieval_plans_state"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plans_lease CHECK (lease_seconds > 0)",
      "expression": "(lease_seconds > 0)",
      "kind": "check",
      "name": "ck_retrieval_plans_lease"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plans_restore_policy CHECK (restore_policy IN ('allow','never'))",
      "expression": "(restore_policy IN ('allow','never'))",
      "kind": "check",
      "name": "ck_retrieval_plans_restore_policy"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plans_file_order CHECK (next_file_order >= 0)",
      "expression": "(next_file_order >= 0)",
      "kind": "check",
      "name": "ck_retrieval_plans_file_order"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plans_creation_identity_sha256_hex CHECK (length(creation_identity_sha256) = 64 AND lower(creation_identity_sha256) = creation_identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(creation_identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(creation_identity_sha256) = 64 AND lower(creation_identity_sha256) = creation_identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(creation_identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_retrieval_plans_creation_identity_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plans_file_commitment_sha256_hex CHECK (length(file_commitment_sha256) = 64 AND lower(file_commitment_sha256) = file_commitment_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(file_commitment_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(file_commitment_sha256) = 64 AND lower(file_commitment_sha256) = file_commitment_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(file_commitment_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_retrieval_plans_file_commitment_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plans_segment_commitment_sha256_hex CHECK (length(segment_commitment_sha256) = 64 AND lower(segment_commitment_sha256) = segment_commitment_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(segment_commitment_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(segment_commitment_sha256) = 64 AND lower(segment_commitment_sha256) = segment_commitment_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(segment_commitment_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_retrieval_plans_segment_commitment_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plans_etag_hex CHECK (etag IS NULL OR length(etag) = 64 AND lower(etag) = etag AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(etag, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(etag IS NULL OR length(etag) = 64 AND lower(etag) = etag AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(etag, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_retrieval_plans_etag_hex"
    }
  ],
  "name": "retrieval_plans"
}
```

</details>
