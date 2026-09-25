# gogurt-listener durable-state identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:gogurt-listener:gogurt-listener-durable-state-identity:5de2995760 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

| Authority fact | Value |
|---|---|
| <a id="s-aeab17033c"></a>`distribution` | `"gogurt-listener-runtime"` |
| <a id="s-45f323bd69"></a>`format` | `"gogurt-listener-state/v1"` |
| <a id="s-6120d9ea81"></a>`head` | `"1"` |
| <a id="s-d8152214b0"></a>`id` | `"gogurt-listener"` |
| <a id="s-4f3a743eae"></a>`dialect` | `"sqlite"` |
| <a id="s-2f406dd2b5"></a>`kind` | `"relational-schema"` |
| <a id="s-a43a4c264d"></a>`unique_indexes` | `[]` |
| <a id="s-0cc49e1a8c"></a>`transition` | `"forward-migration-chain"` |

## Maintained corroboration

### Related interface records

- [dispatches](gogurt-listener-dispatches.md)
- [listener_meta](gogurt-listener-listener-meta.md)
- [observed_mounts](gogurt-listener-observed-mounts.md)

## Governing policies

- <a id="pa-7d835a0142"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:gogurt-listener](../../../evidence/sources/authorities.md#src-6b3ecfced3) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/listener.py::\_LISTENER\_STATE\_DDL](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/listener.py)

### Machine authority

- `/external_contract/durable_state/owners/6/distribution`
- `/external_contract/durable_state/owners/6/format`
- `/external_contract/durable_state/owners/6/head`
- `/external_contract/durable_state/owners/6/id`
- `/external_contract/durable_state/owners/6/structure/dialect`
- `/external_contract/durable_state/owners/6/structure/kind`
- `/external_contract/durable_state/owners/6/structure/unique_indexes`
- `/external_contract/durable_state/owners/6/transition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/durable_state/owners/6/distribution`

<!-- exact-contract-value: c2b374654d33b8c1c63ed15ff754d46047e2deb18d9f2616b6cccc5d9cb4f07d -->

```json
"gogurt-listener-runtime"
```

### `/external_contract/durable_state/owners/6/format`

<!-- exact-contract-value: bd57dc23b8c6620a2ce148f27b330e7b5678840304dd13bf5c03d0ea1bd12724 -->

```json
"gogurt-listener-state/v1"
```

### `/external_contract/durable_state/owners/6/head`

<!-- exact-contract-value: 391552c099c101b131feaf24c5795a6a15bc8ec82015424e0d2b4274a369a0bf -->

```json
"1"
```

### `/external_contract/durable_state/owners/6/id`

<!-- exact-contract-value: 6786c68fb76f81a101d78b72976f8b8bc483ca9c312e63fb9a0f55e80c2d1996 -->

```json
"gogurt-listener"
```

### `/external_contract/durable_state/owners/6/structure/dialect`

<!-- exact-contract-value: 8d52f1b6c789bf660109e29a2036ba4a52615222a8b1ac6f3b94bfdcdc9d93cc -->

```json
"sqlite"
```

### `/external_contract/durable_state/owners/6/structure/kind`

<!-- exact-contract-value: 5ceb771e7cd4b2ae80216231febde0377bb5911894b739ab0612fe91ebf1e3f8 -->

```json
"relational-schema"
```

### `/external_contract/durable_state/owners/6/structure/unique_indexes`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/durable_state/owners/6/transition`

<!-- exact-contract-value: 4618321e15eb5529da1597f7e49b4ca6152dc599ec31740df4d4cd2b5f1d68d3 -->

```json
"forward-migration-chain"
```

</details>
