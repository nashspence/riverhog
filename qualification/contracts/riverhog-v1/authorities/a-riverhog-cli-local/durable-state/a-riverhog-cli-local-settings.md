# a-riverhog-cli-local: settings

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-cli-local:a-riverhog-cli-local-settings:b170dea008 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli-local](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-9454377b33"></a>

### Table: `settings`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-f6e4339d0d"></a>`key` | `TEXT` | no | `—` | — |
| <a id="s-87f15fe289"></a>`value` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-efe74160b5"></a>`primary-key` | `—` | `PRIMARY KEY ("key")` |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-cli-local-durable-state-identity.md)

## Governing policies

- <a id="pa-95b367e130"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-cli-local](../../../evidence/sources/authorities.md#src-196019bfcc) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/1/structure/tables/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
