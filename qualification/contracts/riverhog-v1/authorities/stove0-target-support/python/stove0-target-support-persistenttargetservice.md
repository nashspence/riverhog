# stove0_target_support.PersistentTargetService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-persistenttargetservice:cb3355ec4d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8007380dc4"></a>
- <a id="s-d2e8ccca29"></a>`distribution`: `stove0-target-support`
- <a id="s-7a529f7792"></a>`module`: `stove0_target_support`
- <a id="s-771af501e4"></a>`name`: `PersistentTargetService`
- <a id="s-bb57f1976b"></a>`unit`: `export`

### Declared structure

- <a id="s-0ad1181311"></a>`kind`: `"class"`
- <a id="s-1040ca7098"></a>`signature`: `"\"(*, contract: 'TargetContract', operations: 'Mapping[str, OperationContract]', state_root: 'Path', execute: 'JobExecutor', intent_semantic_validators: 'Mapping[str, IntentSemanticValidator] \| None' = None, maximum_workers: 'int' = 1, terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [cancel_job](stove0-target-support-persistenttargetservice-cancel-job.md)
- [get_job](stove0-target-support-persistenttargetservice-get-job.md)
- [prune_terminal_state](stove0-target-support-persistenttargetservice-prune-terminal-state.md)
- [put_job](stove0-target-support-persistenttargetservice-put-job.md)
- [preflight](stove0-target-support-persistenttargetservice-preflight.md)
- [contract](stove0-target-support-persistenttargetservice-contract.md)
- [close](stove0-target-support-persistenttargetservice-close.md)

## Governing policies

- <a id="pa-58bcde2dcb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.PersistentTargetService`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa90a92595e0bdaf5f5f638f9bb86c7c527d8831be98174a9ecb364a76512175 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, contract: 'TargetContract', operations: 'Mapping[str, OperationContract]', state_root: 'Path', execute: 'JobExecutor', intent_semantic_validators: 'Mapping[str, IntentSemanticValidator] | None' = None, maximum_workers: 'int' = 1, terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "PersistentTargetService",
  "unit": "export"
}
```

</details>
