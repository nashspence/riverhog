# a_stove0_opus_target.OpusTargetService.prune_terminal_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-opus-target:a-stove0-opus-target-opustargetservice-pr-23b9c336fd:ef422c3eb8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-94ba666eaa"></a>
- <a id="s-0a042f93be"></a>`distribution`: `a-stove0-opus-target`
- <a id="s-afa34320b3"></a>`module`: `a_stove0_opus_target`
- <a id="s-2903a3055f"></a>`name`: `prune_terminal_state`
- <a id="s-871fe322a9"></a>`owner`: `a_stove0_opus_target.OpusTargetService`
- <a id="s-e89ab9a844"></a>`unit`: `member`

### Declared structure

- <a id="s-d50696c5c4"></a>`kind`: `"method"`
- <a id="s-8317d6b33e"></a>`signature`: `"\"(self, *, now: 'float \| None' = None) -> 'dict[str, int]'\""`

## Maintained corroboration

### Related interface records

- [OpusTargetService](a-stove0-opus-target-opustargetservice.md)

## Governing policies

- <a id="pa-3f67195526"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-opus-target:a_stove0_opus_target](../../../evidence/sources/authorities.md#src-43f70c74af) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_opus_target.OpusTargetService.prune_terminal_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 129b70de6e0d186fae5a2c8f30bf4934e006b6c5ca64819223b92f2a6a0e75a3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, now: 'float | None' = None) -> 'dict[str, int]'\""
  },
  "distribution": "a-stove0-opus-target",
  "module": "a_stove0_opus_target",
  "name": "prune_terminal_state",
  "owner": "a_stove0_opus_target.OpusTargetService",
  "unit": "member"
}
```

</details>
