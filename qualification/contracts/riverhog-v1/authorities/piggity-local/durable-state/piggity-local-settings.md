# piggity-local: settings

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:piggity-local:piggity-local-settings:fd2e6ff4d8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity-local](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-9454377b33"></a>
- Table: `settings`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-f6e4339d0d"></a>`key` | `TEXT` | no | `—` | — |
| <a id="s-87f15fe289"></a>`value` | `TEXT` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-efe74160b5"></a>`primary-key` | `—` | `PRIMARY KEY ("key")` |

## Maintained corroboration

### Related interface records

- [Schema identity](piggity-local-durable-state-identity.md)

## Governing policies

- <a id="pa-30fa3a7658"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:piggity-local](../../../evidence/sources.md#src-f6a1289f67) — `reference/riverhog/applications/piggity/src/piggity/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/1/structure/tables/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40f8d1fe005839fcbeef6920cb00f89f62e6dd5f313448ed8f4b4d7e04e1dca2 -->

```json
{
  "columns": [
    {
      "definition": "\"key\" TEXT NOT NULL",
      "name": "key",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "value TEXT NOT NULL",
      "name": "value",
      "nullable": false,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "key"
      ],
      "definition": "PRIMARY KEY (\"key\")",
      "kind": "primary-key"
    }
  ],
  "name": "settings"
}
```
