# riverhog-catalog: collection_processing_disposition_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-processing-di-16f227d3a6:65819cb0c3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-7e8c29b350"></a>

### Table: `collection_processing_disposition_outputs`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-36868ea5f3"></a>`claim_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-d82560e79a"></a>`output_path` | `VARCHAR` | no | `—` | — |
| <a id="s-fac02d09a7"></a>`input_collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-3a9681eaec"></a>`input_path` | `VARCHAR` | no | `—` | — |
| <a id="s-1b00351fba"></a>`output_order` | `BIGINT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-4cbbc2098b"></a>`primary-key` | `—` | `PRIMARY KEY (claim_id, output_path, input_collection_id, input_path)` |
| <a id="s-b3065dd1d1"></a>`foreign-key` | `—` | `FOREIGN KEY(claim_id, input_collection_id, input_path) REFERENCES collection_processing_dispositions (claim_id, collection_id, path) ON DELETE CASCADE` |
| <a id="s-6981d4331b"></a>`check` | `ck_processing_disposition_outputs_order_nonnegative` | `CONSTRAINT ck_processing_disposition_outputs_order_nonnegative CHECK (output_order IS NULL OR output_order >= 0)` |
| <a id="s-836c83ae50"></a>`check` | `ck_collection_processing_disposition_outputs_claim_id_hex` | `CONSTRAINT ck_collection_processing_disposition_outputs_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-f8d73c2589"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/82`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6b7d7018f1bf28a1110b0b6b605034e031c8a96a55a9abdc3460364adce67dd1 -->

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
      "definition": "output_path VARCHAR NOT NULL",
      "name": "output_path",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "input_collection_id BIGINT NOT NULL",
      "name": "input_collection_id",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "input_path VARCHAR NOT NULL",
      "name": "input_path",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "output_order BIGINT",
      "name": "output_order",
      "nullable": true,
      "type": "BIGINT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "claim_id",
        "output_path",
        "input_collection_id",
        "input_path"
      ],
      "definition": "PRIMARY KEY (claim_id, output_path, input_collection_id, input_path)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "claim_id",
        "input_collection_id",
        "input_path"
      ],
      "definition": "FOREIGN KEY(claim_id, input_collection_id, input_path) REFERENCES collection_processing_dispositions (claim_id, collection_id, path) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "claim_id",
          "collection_id",
          "path"
        ],
        "table": "collection_processing_dispositions"
      }
    },
    {
      "definition": "CONSTRAINT ck_processing_disposition_outputs_order_nonnegative CHECK (output_order IS NULL OR output_order >= 0)",
      "expression": "(output_order IS NULL OR output_order >= 0)",
      "kind": "check",
      "name": "ck_processing_disposition_outputs_order_nonnegative"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_disposition_outputs_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_disposition_outputs_claim_id_hex"
    }
  ],
  "name": "collection_processing_disposition_outputs"
}
```

</details>
