# a-riverhog-minisign-witness-ledger durable-state identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-minisign-witness-ledger:a-riverhog-minisign-witness-ledger-durabl-06c407e355:a1a709b6a2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-minisign-witness-ledger](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

| Authority fact | Value |
|---|---|
| <a id="s-a4771ded6e"></a>`distribution` | `"a-riverhog-minisign-witness"` |
| <a id="s-c2ee1fed96"></a>`format` | `"state-schema/sqlite"` |
| <a id="s-7fab4606ba"></a>`head` | `"v1_0001"` |
| <a id="s-c8924a76aa"></a>`id` | `"a-riverhog-minisign-witness-ledger"` |
| <a id="s-e2edfe4f15"></a>`dialect` | `"sqlite"` |
| <a id="s-4a6f05cbf1"></a>`kind` | `"relational-schema"` |
| <a id="s-5087e0cd45"></a>`unique_indexes` | `[]` |
| <a id="s-66878c36de"></a>`transition` | `"forward-migration-chain"` |

## Maintained corroboration

### Related interface records

- [observations](a-riverhog-minisign-witness-ledger-observations.md)
- [progress](a-riverhog-minisign-witness-ledger-progress.md)
- [statements](a-riverhog-minisign-witness-ledger-statements.md)

## Governing policies

- <a id="pa-1ce345191f"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-minisign-witness-ledger](../../../evidence/sources/authorities.md#src-d43dd7e522) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a\_riverhog\_minisign\_witness/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a_riverhog_minisign_witness/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/3/distribution`
- `/external_contract/durable_state/owners/3/format`
- `/external_contract/durable_state/owners/3/head`
- `/external_contract/durable_state/owners/3/id`
- `/external_contract/durable_state/owners/3/structure/dialect`
- `/external_contract/durable_state/owners/3/structure/kind`
- `/external_contract/durable_state/owners/3/structure/unique_indexes`
- `/external_contract/durable_state/owners/3/transition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/durable_state/owners/3/distribution`

<!-- exact-contract-value: e0e56be41cc086f192911e756856fca6a8ef627c90f60d3c462e405108bd0afb -->

```json
"a-riverhog-minisign-witness"
```

### `/external_contract/durable_state/owners/3/format`

<!-- exact-contract-value: 212450e0c46f38ac8c8f1d619c8ab3d599af3a7b2ddbaa191900a014edd97529 -->

```json
"state-schema/sqlite"
```

### `/external_contract/durable_state/owners/3/head`

<!-- exact-contract-value: 60903e5b6c89d52dd0ad1ac39a0226db7dccee2c2021bad5d97dce8e5e911f51 -->

```json
"v1_0001"
```

### `/external_contract/durable_state/owners/3/id`

<!-- exact-contract-value: edf36902f2a99a5b1dc7969633e93979b4c5c6690b973e4eceed83c9db1b1428 -->

```json
"a-riverhog-minisign-witness-ledger"
```

### `/external_contract/durable_state/owners/3/structure/dialect`

<!-- exact-contract-value: 8d52f1b6c789bf660109e29a2036ba4a52615222a8b1ac6f3b94bfdcdc9d93cc -->

```json
"sqlite"
```

### `/external_contract/durable_state/owners/3/structure/kind`

<!-- exact-contract-value: 5ceb771e7cd4b2ae80216231febde0377bb5911894b739ab0612fe91ebf1e3f8 -->

```json
"relational-schema"
```

### `/external_contract/durable_state/owners/3/structure/unique_indexes`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/durable_state/owners/3/transition`

<!-- exact-contract-value: 4618321e15eb5529da1597f7e49b4ca6152dc599ec31740df4d4cd2b5f1d68d3 -->

```json
"forward-migration-chain"
```

</details>
