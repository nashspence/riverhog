# mango-fish-cursor durable-state identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:mango-fish-cursor:mango-fish-cursor-durable-state-identity:d85915405d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish-cursor](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

| Authority fact | Value |
|---|---|
| <a id="s-9bc86493fe"></a>`distribution` | `"mango-fish"` |
| <a id="s-0773785869"></a>`format` | `"state-schema/sqlite"` |
| <a id="s-13db1ae0f6"></a>`head` | `"v1_0001"` |
| <a id="s-af7d97db3b"></a>`id` | `"mango-fish-cursor"` |
| <a id="s-1d7a8fec2f"></a>`dialect` | `"sqlite"` |
| <a id="s-76cba99cbb"></a>`kind` | `"relational-schema"` |
| <a id="s-c139d2d53c"></a>`unique_indexes` | `[]` |
| <a id="s-0061bfb0bd"></a>`transition` | `"forward-migration-chain"` |

## Maintained corroboration

### Related interface records

- [source_cursors](mango-fish-cursor-source-cursors.md)

## Governing policies

- <a id="pa-2e74142c90"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:mango-fish-cursor](../../../evidence/sources.md#src-b1cc215b8d) — [reference/riverhog/applications/mango-fish/src/mango\_fish/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../reference/riverhog/applications/mango-fish/src/mango_fish/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/2/distribution`
- `/external_contract/durable_state/owners/2/format`
- `/external_contract/durable_state/owners/2/head`
- `/external_contract/durable_state/owners/2/id`
- `/external_contract/durable_state/owners/2/structure/dialect`
- `/external_contract/durable_state/owners/2/structure/kind`
- `/external_contract/durable_state/owners/2/structure/unique_indexes`
- `/external_contract/durable_state/owners/2/transition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/durable_state/owners/2/distribution`

<!-- exact-contract-value: 1597bf55bb7889ac7479259c9f4631adf1e2188403282e8d82efa49b1815e160 -->

```json
"mango-fish"
```

### `/external_contract/durable_state/owners/2/format`

<!-- exact-contract-value: 212450e0c46f38ac8c8f1d619c8ab3d599af3a7b2ddbaa191900a014edd97529 -->

```json
"state-schema/sqlite"
```

### `/external_contract/durable_state/owners/2/head`

<!-- exact-contract-value: 60903e5b6c89d52dd0ad1ac39a0226db7dccee2c2021bad5d97dce8e5e911f51 -->

```json
"v1_0001"
```

### `/external_contract/durable_state/owners/2/id`

<!-- exact-contract-value: f524508ef3cd3b336b2e28d4b137fbe0a0ef5004abe185b1b2ab3af0e2a93d51 -->

```json
"mango-fish-cursor"
```

### `/external_contract/durable_state/owners/2/structure/dialect`

<!-- exact-contract-value: 8d52f1b6c789bf660109e29a2036ba4a52615222a8b1ac6f3b94bfdcdc9d93cc -->

```json
"sqlite"
```

### `/external_contract/durable_state/owners/2/structure/kind`

<!-- exact-contract-value: 5ceb771e7cd4b2ae80216231febde0377bb5911894b739ab0612fe91ebf1e3f8 -->

```json
"relational-schema"
```

### `/external_contract/durable_state/owners/2/structure/unique_indexes`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/durable_state/owners/2/transition`

<!-- exact-contract-value: 4618321e15eb5529da1597f7e49b4ca6152dc599ec31740df4d4cd2b5f1d68d3 -->

```json
"forward-migration-chain"
```

</details>
