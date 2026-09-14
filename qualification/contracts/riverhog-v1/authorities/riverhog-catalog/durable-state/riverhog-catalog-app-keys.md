# riverhog-catalog: app_keys

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-app-keys:dbb9820649 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-578253b2fe"></a>
- Table: `app_keys`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-e44fb490ed"></a>`id` | `VARCHAR` | no | `—` | — |
| <a id="s-783c2ae605"></a>`app` | `VARCHAR` | no | `—` | — |
| <a id="s-22ee02e7b4"></a>`token_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-9e3258cae2"></a>`monthly_download_quota_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-1d3e2eea6e"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-f528281c4f"></a>`expires_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-0bea5f8cba"></a>`revoked_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-078b54c5df"></a>`last_used_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-bd792d9e56"></a>`search_text` | `VARCHAR` | no | `—` | {"generated":"GENERATED ALWAYS AS (lower(app \|\| ' ' \|\| id)) STORED"} |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-3250a87917"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-8d6e1f6ff9"></a>`check` | `ck_app_keys_download_quota` | `CONSTRAINT ck_app_keys_download_quota CHECK (monthly_download_quota_bytes IS NULL OR monthly_download_quota_bytes >= 0)` |
| <a id="s-0c0b741b50"></a>`check` | `ck_app_keys_token_sha256` | `CONSTRAINT ck_app_keys_token_sha256 CHECK (length(token_sha256) = 64)` |
| <a id="s-9ac08b25f4"></a>`check` | `ck_app_keys_token_sha256_hex` | `CONSTRAINT ck_app_keys_token_sha256_hex CHECK (length(token_sha256) = 64 AND lower(token_sha256) = token_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(token_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-bbe51cde6f"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25bdc4923558eac075fe6e65c65416945769ca6e2e7989115bee08f8ec6f1dad -->

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
      "definition": "token_sha256 VARCHAR(64) NOT NULL",
      "name": "token_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "monthly_download_quota_bytes BIGINT",
      "name": "monthly_download_quota_bytes",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "expires_at VARCHAR",
      "name": "expires_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "revoked_at VARCHAR",
      "name": "revoked_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "last_used_at VARCHAR",
      "name": "last_used_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "search_text VARCHAR GENERATED ALWAYS AS (lower(app || ' ' || id)) STORED NOT NULL",
      "generated": "GENERATED ALWAYS AS (lower(app || ' ' || id)) STORED",
      "name": "search_text",
      "nullable": false,
      "type": "VARCHAR"
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
      "definition": "CONSTRAINT ck_app_keys_download_quota CHECK (monthly_download_quota_bytes IS NULL OR monthly_download_quota_bytes >= 0)",
      "expression": "(monthly_download_quota_bytes IS NULL OR monthly_download_quota_bytes >= 0)",
      "kind": "check",
      "name": "ck_app_keys_download_quota"
    },
    {
      "definition": "CONSTRAINT ck_app_keys_token_sha256 CHECK (length(token_sha256) = 64)",
      "expression": "(length(token_sha256) = 64)",
      "kind": "check",
      "name": "ck_app_keys_token_sha256"
    },
    {
      "definition": "CONSTRAINT ck_app_keys_token_sha256_hex CHECK (length(token_sha256) = 64 AND lower(token_sha256) = token_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(token_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(token_sha256) = 64 AND lower(token_sha256) = token_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(token_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_app_keys_token_sha256_hex"
    }
  ],
  "name": "app_keys"
}
```
