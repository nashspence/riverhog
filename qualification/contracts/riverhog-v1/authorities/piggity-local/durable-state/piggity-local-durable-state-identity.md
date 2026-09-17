# piggity-local durable-state identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:piggity-local:piggity-local-durable-state-identity:54daba4c6d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity-local](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

| Authority fact | Value |
|---|---|
| <a id="s-da06a92e18"></a>`distribution` | `"piggity"` |
| <a id="s-48380901bc"></a>`format` | `"state-schema/sqlite"` |
| <a id="s-5a82d47ada"></a>`head` | `"v1_0001"` |
| <a id="s-dfc86b1403"></a>`id` | `"piggity-local"` |
| <a id="s-d9507697a5"></a>`dialect` | `"sqlite"` |
| <a id="s-9deaf5833f"></a>`kind` | `"relational-schema"` |
| <a id="s-beee00af2c"></a>`unique_indexes` | `[]` |
| <a id="s-e4e9b05c9f"></a>`transition` | `"forward-migration-chain"` |

## Maintained corroboration

### Related interface records

- [desired_collection_tags](piggity-local-desired-collection-tags.md)
- [desired_collections](piggity-local-desired-collections.md)
- [desired_files](piggity-local-desired-files.md)
- [retrieval_job_files](piggity-local-retrieval-job-files.md)
- [retrieval_jobs](piggity-local-retrieval-jobs.md)
- [settings](piggity-local-settings.md)

## Governing policies

- <a id="pa-f61cf95fd5"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:piggity-local](../../../evidence/sources/authorities.md#src-f6a1289f67) — [reference/riverhog/applications/piggity/src/piggity/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../reference/riverhog/applications/piggity/src/piggity/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/1/distribution`
- `/external_contract/durable_state/owners/1/format`
- `/external_contract/durable_state/owners/1/head`
- `/external_contract/durable_state/owners/1/id`
- `/external_contract/durable_state/owners/1/structure/dialect`
- `/external_contract/durable_state/owners/1/structure/kind`
- `/external_contract/durable_state/owners/1/structure/unique_indexes`
- `/external_contract/durable_state/owners/1/transition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/durable_state/owners/1/distribution`

<!-- exact-contract-value: e19ec9c376ac7f2b93d8f395c47794186488a3a41ca96da52aa308203f6069f6 -->

```json
"piggity"
```

### `/external_contract/durable_state/owners/1/format`

<!-- exact-contract-value: 212450e0c46f38ac8c8f1d619c8ab3d599af3a7b2ddbaa191900a014edd97529 -->

```json
"state-schema/sqlite"
```

### `/external_contract/durable_state/owners/1/head`

<!-- exact-contract-value: 60903e5b6c89d52dd0ad1ac39a0226db7dccee2c2021bad5d97dce8e5e911f51 -->

```json
"v1_0001"
```

### `/external_contract/durable_state/owners/1/id`

<!-- exact-contract-value: 69979f43f64692d89404dc75ec637f183000213620edca54ddee5c3a68c50968 -->

```json
"piggity-local"
```

### `/external_contract/durable_state/owners/1/structure/dialect`

<!-- exact-contract-value: 8d52f1b6c789bf660109e29a2036ba4a52615222a8b1ac6f3b94bfdcdc9d93cc -->

```json
"sqlite"
```

### `/external_contract/durable_state/owners/1/structure/kind`

<!-- exact-contract-value: 5ceb771e7cd4b2ae80216231febde0377bb5911894b739ab0612fe91ebf1e3f8 -->

```json
"relational-schema"
```

### `/external_contract/durable_state/owners/1/structure/unique_indexes`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/durable_state/owners/1/transition`

<!-- exact-contract-value: 4618321e15eb5529da1597f7e49b4ca6152dc599ec31740df4d4cd2b5f1d68d3 -->

```json
"forward-migration-chain"
```

</details>
