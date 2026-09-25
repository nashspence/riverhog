# riverhog-catalog: collection_processing_claim_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-processing-cl-b761c1c7be:09ab998132 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-5e15c17a39"></a>

### Table: `collection_processing_claim_artifacts`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-e372eb26e2"></a>`claim_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-4b768c7212"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-5371ae3ac6"></a>`path` | `VARCHAR` | no | `—` | — |
| <a id="s-60e49570cd"></a>`artifact_order` | `BIGINT` | no | `—` | — |
| <a id="s-2c48cc5719"></a>`bytes` | `BIGINT` | no | `—` | — |
| <a id="s-c7b45d8f32"></a>`sha256` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-8e05875b86"></a>`primary-key` | `—` | `PRIMARY KEY (claim_id, collection_id, path)` |
| <a id="s-ac6db663d5"></a>`foreign-key` | `—` | `FOREIGN KEY(claim_id, collection_id) REFERENCES collection_processing_claim_inputs (claim_id, collection_id) ON DELETE CASCADE` |
| <a id="s-1c5bd8767a"></a>`check` | `ck_processing_claim_artifacts_bytes` | `CONSTRAINT ck_processing_claim_artifacts_bytes CHECK (bytes >= 0)` |
| <a id="s-007f57e27f"></a>`check` | `ck_processing_claim_artifacts_order` | `CONSTRAINT ck_processing_claim_artifacts_order CHECK (artifact_order >= 0)` |
| <a id="s-783a406675"></a>`check` | `ck_processing_claim_artifacts_sha256` | `CONSTRAINT ck_processing_claim_artifacts_sha256 CHECK (length(sha256) = 64)` |
| <a id="s-629d80da66"></a>`check` | `ck_collection_processing_claim_artifacts_claim_id_hex` | `CONSTRAINT ck_collection_processing_claim_artifacts_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-b8d9d91daa"></a>`check` | `ck_collection_processing_claim_artifacts_sha256_hex` | `CONSTRAINT ck_collection_processing_claim_artifacts_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-4975fed30e"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/72`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b88a3491d3c70c883331f54d30fa2f7b4433942942f3ae435b9e787b89c528ce -->

```json
{
  "columns": [
    {
      "definition": "claim_id VARCHAR(64) NOT NULL",
      "name": "claim_id",
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
      "definition": "path VARCHAR NOT NULL",
      "name": "path",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "artifact_order BIGINT NOT NULL",
      "name": "artifact_order",
      "nullable": false,
      "type": "BIGINT"
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
    }
  ],
  "constraints": [
    {
      "columns": [
        "claim_id",
        "collection_id",
        "path"
      ],
      "definition": "PRIMARY KEY (claim_id, collection_id, path)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "claim_id",
        "collection_id"
      ],
      "definition": "FOREIGN KEY(claim_id, collection_id) REFERENCES collection_processing_claim_inputs (claim_id, collection_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "claim_id",
          "collection_id"
        ],
        "table": "collection_processing_claim_inputs"
      }
    },
    {
      "definition": "CONSTRAINT ck_processing_claim_artifacts_bytes CHECK (bytes >= 0)",
      "expression": "(bytes >= 0)",
      "kind": "check",
      "name": "ck_processing_claim_artifacts_bytes"
    },
    {
      "definition": "CONSTRAINT ck_processing_claim_artifacts_order CHECK (artifact_order >= 0)",
      "expression": "(artifact_order >= 0)",
      "kind": "check",
      "name": "ck_processing_claim_artifacts_order"
    },
    {
      "definition": "CONSTRAINT ck_processing_claim_artifacts_sha256 CHECK (length(sha256) = 64)",
      "expression": "(length(sha256) = 64)",
      "kind": "check",
      "name": "ck_processing_claim_artifacts_sha256"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claim_artifacts_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_claim_artifacts_claim_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claim_artifacts_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_claim_artifacts_sha256_hex"
    }
  ],
  "name": "collection_processing_claim_artifacts"
}
```

</details>
