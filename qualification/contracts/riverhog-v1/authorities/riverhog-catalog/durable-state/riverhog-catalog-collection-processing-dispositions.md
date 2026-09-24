# riverhog-catalog: collection_processing_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-processing-dispositions:83cbc99e7e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-e666abd22e"></a>

### Table: `collection_processing_dispositions`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-850d878517"></a>`claim_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-200a837398"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-07eb624d96"></a>`path` | `VARCHAR` | no | `—` | — |
| <a id="s-b803f5e5ba"></a>`disposition_order` | `BIGINT` | yes | `—` | — |
| <a id="s-156946567e"></a>`status` | `VARCHAR` | no | `—` | — |
| <a id="s-da6130dfb0"></a>`failure_code` | `VARCHAR` | yes | `—` | — |
| <a id="s-3c0493031e"></a>`failure_message` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-0604a034db"></a>`primary-key` | `—` | `PRIMARY KEY (claim_id, collection_id, path)` |
| <a id="s-112a664b68"></a>`foreign-key` | `—` | `FOREIGN KEY(claim_id, collection_id, path) REFERENCES collection_processing_claim_artifacts (claim_id, collection_id, path) ON DELETE CASCADE` |
| <a id="s-913026d94c"></a>`check` | `ck_processing_dispositions_status` | `CONSTRAINT ck_processing_dispositions_status CHECK (status IN ('transformed','preserved','omitted','rejected'))` |
| <a id="s-d58b710bdc"></a>`check` | `ck_processing_dispositions_order_nonnegative` | `CONSTRAINT ck_processing_dispositions_order_nonnegative CHECK (disposition_order IS NULL OR disposition_order >= 0)` |
| <a id="s-0ebddc89e0"></a>`check` | `ck_collection_processing_dispositions_claim_id_hex` | `CONSTRAINT ck_collection_processing_dispositions_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-ccd49f9902"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/77`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 04a139ddb3c67cf37891fea81616af79703ca0795353744551dedf392f80505f -->

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
      "definition": "disposition_order BIGINT",
      "name": "disposition_order",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "status VARCHAR NOT NULL",
      "name": "status",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "failure_code VARCHAR",
      "name": "failure_code",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "failure_message TEXT",
      "name": "failure_message",
      "nullable": true,
      "type": "TEXT"
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
        "collection_id",
        "path"
      ],
      "definition": "FOREIGN KEY(claim_id, collection_id, path) REFERENCES collection_processing_claim_artifacts (claim_id, collection_id, path) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "claim_id",
          "collection_id",
          "path"
        ],
        "table": "collection_processing_claim_artifacts"
      }
    },
    {
      "definition": "CONSTRAINT ck_processing_dispositions_status CHECK (status IN ('transformed','preserved','omitted','rejected'))",
      "expression": "(status IN ('transformed','preserved','omitted','rejected'))",
      "kind": "check",
      "name": "ck_processing_dispositions_status"
    },
    {
      "definition": "CONSTRAINT ck_processing_dispositions_order_nonnegative CHECK (disposition_order IS NULL OR disposition_order >= 0)",
      "expression": "(disposition_order IS NULL OR disposition_order >= 0)",
      "kind": "check",
      "name": "ck_processing_dispositions_order_nonnegative"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_dispositions_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_dispositions_claim_id_hex"
    }
  ],
  "name": "collection_processing_dispositions"
}
```

</details>
