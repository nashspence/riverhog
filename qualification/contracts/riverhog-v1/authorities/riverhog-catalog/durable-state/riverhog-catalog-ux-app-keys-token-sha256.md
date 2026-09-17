# riverhog-catalog: ux_app_keys_token_sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-ux-app-keys-token-sha256:a4f0d6acfb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-446f2bee98"></a>

| Index fact | Value |
|---|---|
| `columns` | `["token_sha256"]` |
| `definition` | `"CREATE UNIQUE INDEX ux_app_keys_token_sha256 ON app_keys (token_sha256)"` |
| `name` | `"ux_app_keys_token_sha256"` |
| `table` | `"app_keys"` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-3c416c81b9"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/unique_indexes/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c39375f91cc025ebb98a4620f060c640c405965d200807bc5bea5b59dcbc70c -->

```json
{
  "columns": [
    "token_sha256"
  ],
  "definition": "CREATE UNIQUE INDEX ux_app_keys_token_sha256 ON app_keys (token_sha256)",
  "name": "ux_app_keys_token_sha256",
  "table": "app_keys"
}
```

</details>
