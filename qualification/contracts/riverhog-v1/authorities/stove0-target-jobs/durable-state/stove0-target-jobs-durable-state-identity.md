# stove0-target-jobs durable-state identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-target-jobs:stove0-target-jobs-durable-state-identity:ef98cbecb5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-jobs](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

| Authority fact | Value |
|---|---|
| <a id="s-6ccc939d49"></a>`distribution` | `"stove0-target-support"` |
| <a id="s-b41f5a4710"></a>`format` | `"stove0-target-job-state/v1"` |
| <a id="s-20befdd158"></a>`head` | `"v1"` |
| <a id="s-7f0d8ec979"></a>`id` | `"stove0-target-jobs"` |
| <a id="s-89bd43a58e"></a>`kind` | `"json-documents"` |
| <a id="s-dd040d58be"></a>`transition` | `"backward-readable-documents"` |

## Maintained corroboration

### Related interface records

- [AcceptedTargetJob](stove0-target-jobs-acceptedtargetjob.md)
- [TargetJobStatus](stove0-target-jobs-targetjobstatus.md)

## Governing policies

- <a id="pa-7b70892ece"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-target-jobs](../../../evidence/sources.md#src-7b4138829a) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/protocol.py::AcceptedTargetJob](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/protocol.py); [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/protocol.py::TargetJobStatus](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/protocol.py)

### Machine authority

- `/external_contract/durable_state/owners/5/distribution`
- `/external_contract/durable_state/owners/5/format`
- `/external_contract/durable_state/owners/5/head`
- `/external_contract/durable_state/owners/5/id`
- `/external_contract/durable_state/owners/5/structure/kind`
- `/external_contract/durable_state/owners/5/transition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/durable_state/owners/5/distribution`

<!-- exact-contract-value: 6aafb19f5422f7e6a5dd231a829f57e39bbcf35c4e4762df1152daba55a970e4 -->

```json
"stove0-target-support"
```

### `/external_contract/durable_state/owners/5/format`

<!-- exact-contract-value: 5f0091c2bcbd0414b5901e5301f3e4a22bab274b666a754d656e088a752d45c3 -->

```json
"stove0-target-job-state/v1"
```

### `/external_contract/durable_state/owners/5/head`

<!-- exact-contract-value: 161078e42e8fef3ba4b9c984035baa2e431a50b31a18ac95614cc6820394af13 -->

```json
"v1"
```

### `/external_contract/durable_state/owners/5/id`

<!-- exact-contract-value: 7bcf973b8042d2250a6981db36f1fd02b7bacd0e9de80a61e513187418b01d37 -->

```json
"stove0-target-jobs"
```

### `/external_contract/durable_state/owners/5/structure/kind`

<!-- exact-contract-value: 491b02422827494622a560b792ebfcdb97aec09da52537d41ffec7b3fd02f252 -->

```json
"json-documents"
```

### `/external_contract/durable_state/owners/5/transition`

<!-- exact-contract-value: d40b2eb8aff31b7b5e876bd3f0f82e1968a5520ea99afe32514134d4f988d7c7 -->

```json
"backward-readable-documents"
```

</details>
