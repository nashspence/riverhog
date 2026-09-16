# riverhog-catalog: retrieval_plans

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-plans:db49f019b1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-a2c14f3731"></a>

### Table: `retrieval_plans`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-4746cd74e6"></a>`id` | `VARCHAR` | no | `—` | — |
| <a id="s-ab1db00bfb"></a>`app` | `VARCHAR` | no | `—` | — |
| <a id="s-75af01c0c0"></a>`initiated_by_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-1be2709ecf"></a>`idempotency_key` | `VARCHAR` | no | `—` | — |
| <a id="s-d0add89604"></a>`creation_identity_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-05c5e1fa0f"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-fc5c0a24fc"></a>`request_json` | `TEXT` | no | `—` | — |
| <a id="s-560d19365f"></a>`lease_seconds` | `BIGINT` | no | `—` | — |
| <a id="s-35daadf574"></a>`restore_policy` | `VARCHAR` | no | `—` | — |
| <a id="s-02e2b60c11"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-488da60f6d"></a>`ready_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-d816727e85"></a>`expires_at` | `VARCHAR` | no | `—` | — |
| <a id="s-51a07b362e"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-b97c1f7ae3"></a>`next_file_order` | `INTEGER` | no | `—` | — |
| <a id="s-dfd68f2bf7"></a>`next_placement_sequence` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-4674f1ba69"></a>`object_count` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-daf8a8a086"></a>`retrieval_bytes` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-0e321da73a"></a>`requires_restore` | `BOOLEAN` | no | `—` | — |
| <a id="s-14d49ce8c4"></a>`file_commitment_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-4396f5e28e"></a>`segment_commitment_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-4ae912a563"></a>`etag` | `VARCHAR(64)` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-1ada749bca"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-03ba25055d"></a>`unique` | `uq_retrieval_plans_key_idempotency` | `CONSTRAINT uq_retrieval_plans_key_idempotency UNIQUE (app, initiated_by_key_id, idempotency_key)` |
| <a id="s-34f714e0dd"></a>`check` | `ck_retrieval_plans_state` | `CONSTRAINT ck_retrieval_plans_state CHECK (state IN ('planning','ready','consumed','expired','failed'))` |
| <a id="s-161ec1b5d5"></a>`check` | `ck_retrieval_plans_lease` | `CONSTRAINT ck_retrieval_plans_lease CHECK (lease_seconds > 0)` |
| <a id="s-588cdfc014"></a>`check` | `ck_retrieval_plans_restore_policy` | `CONSTRAINT ck_retrieval_plans_restore_policy CHECK (restore_policy IN ('allow','never'))` |
| <a id="s-c8f2a5e0db"></a>`check` | `ck_retrieval_plans_file_order` | `CONSTRAINT ck_retrieval_plans_file_order CHECK (next_file_order >= 0)` |
| <a id="s-935f22f582"></a>`check` | `ck_retrieval_plans_creation_identity_sha256_hex` | `CONSTRAINT ck_retrieval_plans_creation_identity_sha256_hex CHECK (length(creation_identity_sha256) = 64 AND lower(creation_identity_sha256) = creation_identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(creation_identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-5ddb600065"></a>`check` | `ck_retrieval_plans_file_commitment_sha256_hex` | `CONSTRAINT ck_retrieval_plans_file_commitment_sha256_hex CHECK (length(file_commitment_sha256) = 64 AND lower(file_commitment_sha256) = file_commitment_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(file_commitment_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-4afa15e727"></a>`check` | `ck_retrieval_plans_segment_commitment_sha256_hex` | `CONSTRAINT ck_retrieval_plans_segment_commitment_sha256_hex CHECK (length(segment_commitment_sha256) = 64 AND lower(segment_commitment_sha256) = segment_commitment_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(segment_commitment_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-fd6f299012"></a>`check` | `ck_retrieval_plans_etag_hex` | `CONSTRAINT ck_retrieval_plans_etag_hex CHECK (etag IS NULL OR length(etag) = 64 AND lower(etag) = etag AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(etag, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-7bf72c2aa1"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/15`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2817beb86d63085cb240382fe39a91358cbd4eb6fc72e55b52b54cd6eb76b1ef -->

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
      "definition": "app VARCHAR NOT NULL",
      "name": "app",
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
        "app",
        "initiated_by_key_id",
        "idempotency_key"
      ],
      "definition": "CONSTRAINT uq_retrieval_plans_key_idempotency UNIQUE (app, initiated_by_key_id, idempotency_key)",
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
