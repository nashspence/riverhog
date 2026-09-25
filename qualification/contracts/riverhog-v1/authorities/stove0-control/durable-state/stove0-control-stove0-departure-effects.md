# stove0-control: stove0_departure_effects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-departure-effects:617f31afba -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-a1f3d3f316"></a>

### Table: `stove0_departure_effects`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-d1dde0bb12"></a>`departure_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-d52ca4fe5a"></a>`policy_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-274b7076a7"></a>`state` | `VARCHAR(16)` | no | `—` | — |
| <a id="s-19cf31b5a0"></a>`document_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-dd02e62f6b"></a>`document_json` | `TEXT` | no | `—` | — |
| <a id="s-4560ffeee0"></a>`receipt_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-096e14879a"></a>`receipt_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-3905ca4aed"></a>`receipt_json` | `TEXT` | yes | `—` | — |
| <a id="s-20940a8015"></a>`attempt_count` | `INTEGER` | no | `—` | — |
| <a id="s-64ed940f0c"></a>`next_attempt_at` | `VARCHAR(40)` | yes | `—` | — |
| <a id="s-47c1964fbe"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-231a754073"></a>`created_at` | `VARCHAR(40)` | no | `—` | — |
| <a id="s-aea69b7112"></a>`updated_at` | `VARCHAR(40)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-4793cb2098"></a>`primary-key` | `—` | `PRIMARY KEY (departure_id)` |
| <a id="s-9a6ee8944b"></a>`check` | `ck_stove0_departure_effect_state` | `CONSTRAINT ck_stove0_departure_effect_state CHECK (state IN ('pending','complete'))` |
| <a id="s-e1f47a5341"></a>`check` | `ck_stove0_departure_effect_bytes` | `CONSTRAINT ck_stove0_departure_effect_bytes CHECK (document_bytes >= 0)` |
| <a id="s-9be6cdd04a"></a>`check` | `ck_stove0_departure_effect_receipt_bytes` | `CONSTRAINT ck_stove0_departure_effect_receipt_bytes CHECK (receipt_bytes IS NULL OR receipt_bytes >= 0)` |
| <a id="s-cdd9f700b3"></a>`check` | `ck_stove0_departure_effect_attempts` | `CONSTRAINT ck_stove0_departure_effect_attempts CHECK (attempt_count >= 0)` |
| <a id="s-65158580f6"></a>`check` | `ck_stove0_departure_effect_stage` | `CONSTRAINT ck_stove0_departure_effect_stage CHECK (state = 'complete' AND receipt_sha256 IS NOT NULL AND receipt_bytes IS NOT NULL AND receipt_json IS NOT NULL AND next_attempt_at IS NULL OR state = 'pending' AND receipt_sha256 IS NULL AND receipt_bytes IS NULL AND receipt_json IS NULL AND next_attempt_at IS NOT NULL)` |
| <a id="s-caef63e49f"></a>`check` | `ck_stove0_departure_effects_departure_id_hex` | `CONSTRAINT ck_stove0_departure_effects_departure_id_hex CHECK (length(departure_id) = 64 AND lower(departure_id) = departure_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(departure_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-90907bd21f"></a>`check` | `ck_stove0_departure_effects_receipt_sha256_hex` | `CONSTRAINT ck_stove0_departure_effects_receipt_sha256_hex CHECK (receipt_sha256 IS NULL OR length(receipt_sha256) = 64 AND lower(receipt_sha256) = receipt_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(receipt_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-3d6980ac8a"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/5/structure/tables/2`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5777fee7ce67d4890f99fc19431545a28842f421c068f28d094208a157b7b88c -->

```json
{
  "columns": [
    {
      "definition": "departure_id VARCHAR(64) NOT NULL",
      "name": "departure_id",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "policy_id VARCHAR(160) NOT NULL",
      "name": "policy_id",
      "nullable": false,
      "type": "VARCHAR(160)"
    },
    {
      "definition": "state VARCHAR(16) NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR(16)"
    },
    {
      "definition": "document_bytes BIGINT NOT NULL",
      "name": "document_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "document_json TEXT NOT NULL",
      "name": "document_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "receipt_sha256 VARCHAR(64)",
      "name": "receipt_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "receipt_bytes BIGINT",
      "name": "receipt_bytes",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "receipt_json TEXT",
      "name": "receipt_json",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "attempt_count INTEGER NOT NULL",
      "name": "attempt_count",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "next_attempt_at VARCHAR(40)",
      "name": "next_attempt_at",
      "nullable": true,
      "type": "VARCHAR(40)"
    },
    {
      "definition": "failure TEXT",
      "name": "failure",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "created_at VARCHAR(40) NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR(40)"
    },
    {
      "definition": "updated_at VARCHAR(40) NOT NULL",
      "name": "updated_at",
      "nullable": false,
      "type": "VARCHAR(40)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "departure_id"
      ],
      "definition": "PRIMARY KEY (departure_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_effect_state CHECK (state IN ('pending','complete'))",
      "expression": "(state IN ('pending','complete'))",
      "kind": "check",
      "name": "ck_stove0_departure_effect_state"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_effect_bytes CHECK (document_bytes >= 0)",
      "expression": "(document_bytes >= 0)",
      "kind": "check",
      "name": "ck_stove0_departure_effect_bytes"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_effect_receipt_bytes CHECK (receipt_bytes IS NULL OR receipt_bytes >= 0)",
      "expression": "(receipt_bytes IS NULL OR receipt_bytes >= 0)",
      "kind": "check",
      "name": "ck_stove0_departure_effect_receipt_bytes"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_effect_attempts CHECK (attempt_count >= 0)",
      "expression": "(attempt_count >= 0)",
      "kind": "check",
      "name": "ck_stove0_departure_effect_attempts"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_effect_stage CHECK (state = 'complete' AND receipt_sha256 IS NOT NULL AND receipt_bytes IS NOT NULL AND receipt_json IS NOT NULL AND next_attempt_at IS NULL OR state = 'pending' AND receipt_sha256 IS NULL AND receipt_bytes IS NULL AND receipt_json IS NULL AND next_attempt_at IS NOT NULL)",
      "expression": "(state = 'complete' AND receipt_sha256 IS NOT NULL AND receipt_bytes IS NOT NULL AND receipt_json IS NOT NULL AND next_attempt_at IS NULL OR state = 'pending' AND receipt_sha256 IS NULL AND receipt_bytes IS NULL AND receipt_json IS NULL AND next_attempt_at IS NOT NULL)",
      "kind": "check",
      "name": "ck_stove0_departure_effect_stage"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_effects_departure_id_hex CHECK (length(departure_id) = 64 AND lower(departure_id) = departure_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(departure_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(departure_id) = 64 AND lower(departure_id) = departure_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(departure_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_departure_effects_departure_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_effects_receipt_sha256_hex CHECK (receipt_sha256 IS NULL OR length(receipt_sha256) = 64 AND lower(receipt_sha256) = receipt_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(receipt_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(receipt_sha256 IS NULL OR length(receipt_sha256) = 64 AND lower(receipt_sha256) = receipt_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(receipt_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_departure_effects_receipt_sha256_hex"
    }
  ],
  "name": "stove0_departure_effects"
}
```

</details>
