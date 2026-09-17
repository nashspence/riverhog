# riverhog-ftp-custody durable-state identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-ftp-custody:riverhog-ftp-custody-durable-state-identity:40037ad52e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

| Authority fact | Value |
|---|---|
| <a id="s-aeab17033c"></a>`distribution` | `"riverhog-ftp-adapter"` |
| <a id="s-45f323bd69"></a>`format` | `"riverhog-ftp-adapter-claim/v1"` |
| <a id="s-6120d9ea81"></a>`head` | `"v1"` |
| <a id="s-d8152214b0"></a>`id` | `"riverhog-ftp-custody"` |
| <a id="s-2f406dd2b5"></a>`kind` | `"composite"` |
| <a id="s-0cc49e1a8c"></a>`transition` | `"backward-readable-documents"` |

## Maintained corroboration

### Related interface records

- [claim](riverhog-ftp-custody-claim.md)
- [completion-log](riverhog-ftp-custody-completion-log.md)
- [operational-database](riverhog-ftp-custody-operational-database.md)
- [payload](riverhog-ftp-custody-payload.md)
- [receipt](riverhog-ftp-custody-receipt.md)

## Governing policies

- <a id="pa-22b7a696d2"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-ftp-custody](../../../evidence/sources/authorities.md#src-54f88a3a47) — [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/state\_contract.py::ftp\_custody\_state\_contract](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/state_contract.py)

### Machine authority

- `/external_contract/durable_state/owners/6/distribution`
- `/external_contract/durable_state/owners/6/format`
- `/external_contract/durable_state/owners/6/head`
- `/external_contract/durable_state/owners/6/id`
- `/external_contract/durable_state/owners/6/structure/kind`
- `/external_contract/durable_state/owners/6/transition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/durable_state/owners/6/distribution`

<!-- exact-contract-value: 7e14ae24d5b05761347a785bd381607ae0f3f00e29d17470741f0a64d8f9e3bc -->

```json
"riverhog-ftp-adapter"
```

### `/external_contract/durable_state/owners/6/format`

<!-- exact-contract-value: 4683383434d7e7ce55598a612c7b3fcc9849b5ca4c54b8cc684ef9911d97df53 -->

```json
"riverhog-ftp-adapter-claim/v1"
```

### `/external_contract/durable_state/owners/6/head`

<!-- exact-contract-value: 161078e42e8fef3ba4b9c984035baa2e431a50b31a18ac95614cc6820394af13 -->

```json
"v1"
```

### `/external_contract/durable_state/owners/6/id`

<!-- exact-contract-value: dd88157d1af5a8261aa67ed96fc9d04c3dfd381419a19bcb89514f667f509173 -->

```json
"riverhog-ftp-custody"
```

### `/external_contract/durable_state/owners/6/structure/kind`

<!-- exact-contract-value: d2d4cf08a4befc0ed75da38873a70dbab2fc8fc1039ee50bb98c84c0901d6847 -->

```json
"composite"
```

### `/external_contract/durable_state/owners/6/transition`

<!-- exact-contract-value: d40b2eb8aff31b7b5e876bd3f0f82e1968a5520ea99afe32514134d4f988d7c7 -->

```json
"backward-readable-documents"
```

</details>
