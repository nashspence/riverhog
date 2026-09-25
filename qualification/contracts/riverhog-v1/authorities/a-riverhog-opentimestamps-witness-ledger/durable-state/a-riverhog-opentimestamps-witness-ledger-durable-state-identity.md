# a-riverhog-opentimestamps-witness-ledger durable-state identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-opentimestamps-witness-ledger:a-riverhog-opentimestamps-witness-ledger-82306acd49:89b98dd0a8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness-ledger](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

| Authority fact | Value |
|---|---|
| <a id="s-69f9197408"></a>`distribution` | `"a-riverhog-opentimestamps-witness"` |
| <a id="s-dfdb8345df"></a>`format` | `"state-schema/sqlite"` |
| <a id="s-5f90543a80"></a>`head` | `"v1_0001"` |
| <a id="s-01e00b3419"></a>`id` | `"a-riverhog-opentimestamps-witness-ledger"` |
| <a id="s-92f3c74137"></a>`dialect` | `"sqlite"` |
| <a id="s-9b48522278"></a>`kind` | `"relational-schema"` |
| <a id="s-29e150c4ba"></a>`unique_indexes` | `[]` |
| <a id="s-09f176d7fc"></a>`transition` | `"forward-migration-chain"` |

## Maintained corroboration

### Related interface records

- [proof_history](a-riverhog-opentimestamps-witness-ledger-proof-history.md)
- [observations](a-riverhog-opentimestamps-witness-ledger-observations.md)
- [progress](a-riverhog-opentimestamps-witness-ledger-progress.md)
- [statements](a-riverhog-opentimestamps-witness-ledger-statements.md)

## Governing policies

- <a id="pa-28b19d4a1f"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-opentimestamps-witness-ledger](../../../evidence/sources/authorities.md#src-31e2884ee3) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/4/distribution`
- `/external_contract/durable_state/owners/4/format`
- `/external_contract/durable_state/owners/4/head`
- `/external_contract/durable_state/owners/4/id`
- `/external_contract/durable_state/owners/4/structure/dialect`
- `/external_contract/durable_state/owners/4/structure/kind`
- `/external_contract/durable_state/owners/4/structure/unique_indexes`
- `/external_contract/durable_state/owners/4/transition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/durable_state/owners/4/distribution`

<!-- exact-contract-value: 652525cf7d50c1698da736f4635c1649a675146184fc82ed72664c12a39188b5 -->

```json
"a-riverhog-opentimestamps-witness"
```

### `/external_contract/durable_state/owners/4/format`

<!-- exact-contract-value: 212450e0c46f38ac8c8f1d619c8ab3d599af3a7b2ddbaa191900a014edd97529 -->

```json
"state-schema/sqlite"
```

### `/external_contract/durable_state/owners/4/head`

<!-- exact-contract-value: 60903e5b6c89d52dd0ad1ac39a0226db7dccee2c2021bad5d97dce8e5e911f51 -->

```json
"v1_0001"
```

### `/external_contract/durable_state/owners/4/id`

<!-- exact-contract-value: 445719951e113ce85ccff5f8db933655adc441fb8d443a71ec0a87bd6761b71e -->

```json
"a-riverhog-opentimestamps-witness-ledger"
```

### `/external_contract/durable_state/owners/4/structure/dialect`

<!-- exact-contract-value: 8d52f1b6c789bf660109e29a2036ba4a52615222a8b1ac6f3b94bfdcdc9d93cc -->

```json
"sqlite"
```

### `/external_contract/durable_state/owners/4/structure/kind`

<!-- exact-contract-value: 5ceb771e7cd4b2ae80216231febde0377bb5911894b739ab0612fe91ebf1e3f8 -->

```json
"relational-schema"
```

### `/external_contract/durable_state/owners/4/structure/unique_indexes`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/durable_state/owners/4/transition`

<!-- exact-contract-value: 4618321e15eb5529da1597f7e49b4ca6152dc599ec31740df4d4cd2b5f1d68d3 -->

```json
"forward-migration-chain"
```

</details>
