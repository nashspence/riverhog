# riverhog-catalog: collection_upload_raw_part_digests

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-raw-part-digests:fd85c3cffe -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-6c12389cec"></a>

### Table: `collection_upload_raw_part_digests`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1e83356b20"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-4e108b72da"></a>`path` | `VARCHAR` | no | `—` | — |
| <a id="s-64c0168df7"></a>`part_number` | `BIGINT` | no | `—` | — |
| <a id="s-0a8fb37692"></a>`sha256` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-ce4246d144"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, path, part_number)` |
| <a id="s-dc42421300"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, path) REFERENCES collection_upload_files (collection_id, path) ON DELETE CASCADE` |
| <a id="s-70678ad695"></a>`check` | `ck_upload_raw_part_digest_number` | `CONSTRAINT ck_upload_raw_part_digest_number CHECK (part_number >= 0)` |
| <a id="s-52beae72e1"></a>`check` | `ck_upload_raw_part_digest_sha256` | `CONSTRAINT ck_upload_raw_part_digest_sha256 CHECK (length(sha256) = 64)` |
| <a id="s-4022d650d0"></a>`check` | `ck_collection_upload_raw_part_digests_sha256_hex` | `CONSTRAINT ck_collection_upload_raw_part_digests_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-bac539ca39"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/67`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2e5106542358acd6c7366c52780c983544ac7c19bc90a0c57933f6738bc6c13 -->

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
      "definition": "path VARCHAR NOT NULL",
      "name": "path",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "part_number BIGINT NOT NULL",
      "name": "part_number",
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
        "collection_id",
        "path",
        "part_number"
      ],
      "definition": "PRIMARY KEY (collection_id, path, part_number)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id",
        "path"
      ],
      "definition": "FOREIGN KEY(collection_id, path) REFERENCES collection_upload_files (collection_id, path) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "path"
        ],
        "table": "collection_upload_files"
      }
    },
    {
      "definition": "CONSTRAINT ck_upload_raw_part_digest_number CHECK (part_number >= 0)",
      "expression": "(part_number >= 0)",
      "kind": "check",
      "name": "ck_upload_raw_part_digest_number"
    },
    {
      "definition": "CONSTRAINT ck_upload_raw_part_digest_sha256 CHECK (length(sha256) = 64)",
      "expression": "(length(sha256) = 64)",
      "kind": "check",
      "name": "ck_upload_raw_part_digest_sha256"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_raw_part_digests_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_upload_raw_part_digests_sha256_hex"
    }
  ],
  "name": "collection_upload_raw_part_digests"
}
```

</details>
