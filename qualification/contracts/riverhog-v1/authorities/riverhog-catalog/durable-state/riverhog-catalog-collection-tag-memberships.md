# riverhog-catalog: collection_tag_memberships

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-memberships:17a329847b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-41a27eabce"></a>

### Table: `collection_tag_memberships`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-223457f6a8"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-336a144acc"></a>`tag_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-4137247875"></a>`added_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-f6bebec933"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, tag_sha256)` |
| <a id="s-742acfcd0e"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-9c4daf586c"></a>`foreign-key` | `—` | `FOREIGN KEY(tag_sha256) REFERENCES collection_tags (tag_sha256) ON DELETE CASCADE` |
| <a id="s-ca4308bcf6"></a>`check` | `ck_collection_tag_memberships_tag_sha256_hex` | `CONSTRAINT ck_collection_tag_memberships_tag_sha256_hex CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-056526695b"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/24`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8cf18ef23dc8531d0d54c4961cf73d284aefe549181f3998e68bc0ad61475b56 -->

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
      "definition": "tag_sha256 VARCHAR(64) NOT NULL",
      "name": "tag_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "added_at VARCHAR NOT NULL",
      "name": "added_at",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "tag_sha256"
      ],
      "definition": "PRIMARY KEY (collection_id, tag_sha256)",
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
      "columns": [
        "tag_sha256"
      ],
      "definition": "FOREIGN KEY(tag_sha256) REFERENCES collection_tags (tag_sha256) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "tag_sha256"
        ],
        "table": "collection_tags"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_memberships_tag_sha256_hex CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_memberships_tag_sha256_hex"
    }
  ],
  "name": "collection_tag_memberships"
}
```

</details>
