# riverhog-catalog: ux_collection_uploads_application_idempotency_key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-ux-collection-uploads-ap-9fa192a7a1:843114036b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-4f66667ef9"></a>

| Index fact | Value |
|---|---|
| `columns` | `["initiated_by_app","idempotency_key"]` |
| `definition` | `"CREATE UNIQUE INDEX ux_collection_uploads_application_idempotency_key ON collection_uploads (initiated_by_app, idempotency_key)"` |
| `name` | `"ux_collection_uploads_application_idempotency_key"` |
| `table` | `"collection_uploads"` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-51c5d75cb9"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/unique_indexes/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 69116ecb915317deb428a18b53a67e58072e7c309b1dca3ed36a675126f567fe -->

```json
{
  "columns": [
    "initiated_by_app",
    "idempotency_key"
  ],
  "definition": "CREATE UNIQUE INDEX ux_collection_uploads_application_idempotency_key ON collection_uploads (initiated_by_app, idempotency_key)",
  "name": "ux_collection_uploads_application_idempotency_key",
  "table": "collection_uploads"
}
```

</details>
