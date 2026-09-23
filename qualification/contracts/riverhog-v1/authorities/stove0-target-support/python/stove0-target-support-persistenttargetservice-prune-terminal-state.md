# stove0_target_support.PersistentTargetService.prune_terminal_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-persistenttargetser-3b8d22456c:c2a5ef35d7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aee85ef525"></a>
- <a id="s-27dab9d940"></a>`distribution`: `stove0-target-support`
- <a id="s-abab2743a1"></a>`module`: `stove0_target_support`
- <a id="s-a8f2fd7e53"></a>`name`: `prune_terminal_state`
- <a id="s-afd8153d7a"></a>`owner`: `stove0_target_support.PersistentTargetService`
- <a id="s-3882d4dbd8"></a>`unit`: `member`

### Declared structure

- <a id="s-25bc6182ba"></a>`kind`: `"method"`
- <a id="s-8440598f1d"></a>`signature`: `"\"(self, *, now: 'float \| None' = None) -> 'dict[str, int]'\""`

## Maintained corroboration

### Related interface records

- [PersistentTargetService](stove0-target-support-persistenttargetservice.md)

## Governing policies

- <a id="pa-7fbe63d9a9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.PersistentTargetService.prune_terminal_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 729ee855837158e0caefa604cb12e5dbbcf7d743700e8ab7d8e44121ff0f9a01 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, now: 'float | None' = None) -> 'dict[str, int]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "prune_terminal_state",
  "owner": "stove0_target_support.PersistentTargetService",
  "unit": "member"
}
```

</details>
