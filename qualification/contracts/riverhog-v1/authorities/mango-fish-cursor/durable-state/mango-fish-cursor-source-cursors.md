# mango-fish-cursor: source_cursors

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:mango-fish-cursor:mango-fish-cursor-source-cursors:ff21b5dcb5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish-cursor](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-e3615ea9f9"></a>

### Table: `source_cursors`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-012ecf2384"></a>`source` | `TEXT` | no | `—` | — |
| <a id="s-1fc0f931e2"></a>`cursor` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-08a1372d43"></a>`primary-key` | `—` | `PRIMARY KEY (source)` |

## Maintained corroboration

### Related interface records

- [Schema identity](mango-fish-cursor-durable-state-identity.md)

## Governing policies

- <a id="pa-f193d3d478"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:mango-fish-cursor](../../../evidence/sources.md#src-b1cc215b8d) — `reference/riverhog/applications/mango-fish/src/mango_fish/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/2/structure/tables/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08325ca3ab30c38070d17ec9c1d41487aff277f3bec419aabe8dd85f11baa1f0 -->

```json
{
  "columns": [
    {
      "definition": "source TEXT NOT NULL",
      "name": "source",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "cursor TEXT NOT NULL",
      "name": "cursor",
      "nullable": false,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "source"
      ],
      "definition": "PRIMARY KEY (source)",
      "kind": "primary-key"
    }
  ],
  "name": "source_cursors"
}
```

</details>
