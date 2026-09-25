# a-riverhog-ftp-spool-custody durable-state identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-ftp-spool-custody:a-riverhog-ftp-spool-custody-durable-state-identity:fb169a51d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

| Authority fact | Value |
|---|---|
| <a id="s-ec7df46444"></a>`distribution` | `"a-riverhog-ftp-spool"` |
| <a id="s-17f7d86dfc"></a>`format` | `"a-riverhog-ftp-spool-claim/v1"` |
| <a id="s-d746b6b126"></a>`head` | `"v1"` |
| <a id="s-1c960e6a40"></a>`id` | `"a-riverhog-ftp-spool-custody"` |
| <a id="s-264b2e76ca"></a>`kind` | `"composite"` |
| <a id="s-aa75bc1607"></a>`transition` | `"backward-readable-documents"` |

## Maintained corroboration

### Related interface records

- [claim](a-riverhog-ftp-spool-custody-claim.md)
- [completion-log](a-riverhog-ftp-spool-custody-completion-log.md)
- [operational-database](a-riverhog-ftp-spool-custody-operational-database.md)
- [payload](a-riverhog-ftp-spool-custody-payload.md)
- [receipt](a-riverhog-ftp-spool-custody-receipt.md)

## Governing policies

- <a id="pa-671f448cc5"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-ftp-spool-custody](../../../evidence/sources/authorities.md#src-97232522a5) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/state\_contract.py::ftp\_custody\_state\_contract](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/state_contract.py)

### Machine authority

- `/external_contract/durable_state/owners/8/distribution`
- `/external_contract/durable_state/owners/8/format`
- `/external_contract/durable_state/owners/8/head`
- `/external_contract/durable_state/owners/8/id`
- `/external_contract/durable_state/owners/8/structure/kind`
- `/external_contract/durable_state/owners/8/transition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/durable_state/owners/8/distribution`

<!-- exact-contract-value: dad0a7c651a3726342a74cf71b13d83f48ca68a5c1d5dc4dd8678d8a1d0e0bc3 -->

```json
"a-riverhog-ftp-spool"
```

### `/external_contract/durable_state/owners/8/format`

<!-- exact-contract-value: 0137f3a22a6d9e902f75d849e9ba2f948c50fd730522605d82178897a09d9b27 -->

```json
"a-riverhog-ftp-spool-claim/v1"
```

### `/external_contract/durable_state/owners/8/head`

<!-- exact-contract-value: 161078e42e8fef3ba4b9c984035baa2e431a50b31a18ac95614cc6820394af13 -->

```json
"v1"
```

### `/external_contract/durable_state/owners/8/id`

<!-- exact-contract-value: f0478de2b359f2c565529a1c87c53a3ce1ca2534ddc6b76d28cf0f9399f900b7 -->

```json
"a-riverhog-ftp-spool-custody"
```

### `/external_contract/durable_state/owners/8/structure/kind`

<!-- exact-contract-value: d2d4cf08a4befc0ed75da38873a70dbab2fc8fc1039ee50bb98c84c0901d6847 -->

```json
"composite"
```

### `/external_contract/durable_state/owners/8/transition`

<!-- exact-contract-value: d40b2eb8aff31b7b5e876bd3f0f82e1968a5520ea99afe32514134d4f988d7c7 -->

```json
"backward-readable-documents"
```

</details>
