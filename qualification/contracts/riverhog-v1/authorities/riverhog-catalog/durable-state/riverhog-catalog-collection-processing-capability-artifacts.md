# riverhog-catalog: collection_processing_capability_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-processing-ca-4c6c725d66:18c2c9cddf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-88bf9910ea"></a>

### Table: `collection_processing_capability_artifacts`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-8e986a5d99"></a>`capability_id` | `VARCHAR(32)` | no | `—` | — |
| <a id="s-052416dafd"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-2cf457d11d"></a>`path` | `VARCHAR` | no | `—` | — |
| <a id="s-a5aa6b2d02"></a>`artifact_order` | `BIGINT` | no | `—` | — |
| <a id="s-59c0bfa9ac"></a>`bytes` | `BIGINT` | no | `—` | — |
| <a id="s-11181a55cc"></a>`sha256` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-4ab13d9a44"></a>`primary-key` | `—` | `PRIMARY KEY (capability_id, collection_id, path)` |
| <a id="s-3d92348c64"></a>`check` | `ck_capability_artifacts_bytes` | `CONSTRAINT ck_capability_artifacts_bytes CHECK (bytes >= 0)` |
| <a id="s-986e99e0a7"></a>`check` | `ck_capability_artifacts_order` | `CONSTRAINT ck_capability_artifacts_order CHECK (artifact_order >= 0)` |
| <a id="s-551bd4ca17"></a>`check` | `ck_capability_artifacts_sha256` | `CONSTRAINT ck_capability_artifacts_sha256 CHECK (length(sha256) = 64)` |
| <a id="s-81e5d2d380"></a>`foreign-key` | `—` | `FOREIGN KEY(capability_id) REFERENCES collection_processing_capabilities (id) ON DELETE CASCADE` |
| <a id="s-3e9933a4e0"></a>`check` | `ck_collection_processing_capability_artifacts_sha256_hex` | `CONSTRAINT ck_collection_processing_capability_artifacts_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-40d70582ad"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/73`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6e6d3ce20a6ba1321cb85d259b3a45aca0df7a14841743b889de0c2490bff76 -->

```json
{
  "columns": [
    {
      "definition": "capability_id VARCHAR(32) NOT NULL",
      "name": "capability_id",
      "nullable": false,
      "type": "VARCHAR(32)"
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
        "capability_id",
        "collection_id",
        "path"
      ],
      "definition": "PRIMARY KEY (capability_id, collection_id, path)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_capability_artifacts_bytes CHECK (bytes >= 0)",
      "expression": "(bytes >= 0)",
      "kind": "check",
      "name": "ck_capability_artifacts_bytes"
    },
    {
      "definition": "CONSTRAINT ck_capability_artifacts_order CHECK (artifact_order >= 0)",
      "expression": "(artifact_order >= 0)",
      "kind": "check",
      "name": "ck_capability_artifacts_order"
    },
    {
      "definition": "CONSTRAINT ck_capability_artifacts_sha256 CHECK (length(sha256) = 64)",
      "expression": "(length(sha256) = 64)",
      "kind": "check",
      "name": "ck_capability_artifacts_sha256"
    },
    {
      "columns": [
        "capability_id"
      ],
      "definition": "FOREIGN KEY(capability_id) REFERENCES collection_processing_capabilities (id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "id"
        ],
        "table": "collection_processing_capabilities"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_capability_artifacts_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_capability_artifacts_sha256_hex"
    }
  ],
  "name": "collection_processing_capability_artifacts"
}
```

</details>
