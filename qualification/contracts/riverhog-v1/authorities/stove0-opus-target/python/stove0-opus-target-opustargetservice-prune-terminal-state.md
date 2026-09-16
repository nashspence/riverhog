# stove0_opus_target.OpusTargetService.prune_terminal_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-target:stove0-opus-target-opustargetservice-prun-d9bb1ec920:6a4d2dc602 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-debde0cbf6"></a>
- <a id="s-1b0373090d"></a>`distribution`: `stove0-opus-target`
- <a id="s-37b4c86315"></a>`module`: `stove0_opus_target`
- <a id="s-efd20ea274"></a>`name`: `prune_terminal_state`
- <a id="s-08d1a81408"></a>`owner`: `stove0_opus_target.OpusTargetService`
- <a id="s-de87739158"></a>`unit`: `member`

### Declared structure

- <a id="s-66a0254f50"></a>`kind`: `"method"`
- <a id="s-e0301a2f0f"></a>`signature`: `"\"(self, *, now: 'float \| None' = None) -> 'dict[str, int]'\""`

## Maintained corroboration

### Related interface records

- [OpusTargetService](stove0-opus-target-opustargetservice.md)

## Governing policies

- <a id="pa-9c98746e14"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-opus-target:stove0_opus_target](../../../evidence/sources.md#src-9164f15983) — `reference/stove0/targets/opus/target/src/stove0_opus_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_opus_target.OpusTargetService.prune_terminal_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d96956b9a273b576ceab97a63c2e95b93a9a6a3a8964379c87ecf98fd8a9a18 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, now: 'float | None' = None) -> 'dict[str, int]'\""
  },
  "distribution": "stove0-opus-target",
  "module": "stove0_opus_target",
  "name": "prune_terminal_state",
  "owner": "stove0_opus_target.OpusTargetService",
  "unit": "member"
}
```

</details>
