# stove0_target_protocol.EffectPlan.canonical_observation_results

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-effectplan-canonic-0dcc74d21e:d30de2c9e8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ec3ac35e67"></a>
- <a id="s-ecde130737"></a>`distribution`: `stove0-target-protocol`
- <a id="s-c8f21025d7"></a>`module`: `stove0_target_protocol`
- <a id="s-53772207d7"></a>`name`: `canonical_observation_results`
- <a id="s-915a9edd18"></a>`owner`: `stove0_target_protocol.EffectPlan`
- <a id="s-82201c617b"></a>`unit`: `member`

### Declared structure

- <a id="s-7edf798225"></a>`kind`: `"classmethod"`
- <a id="s-c92e749452"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [EffectPlan](stove0-target-protocol-effectplan.md)

## Governing policies

- <a id="pa-344e2fcd76"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.EffectPlan.canonical_observation_results`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e176e9830473bccc22f7c55a1172a99d3035657b0e10a0d2e9ec20a7ad0b2de -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_observation_results",
  "owner": "stove0_target_protocol.EffectPlan",
  "unit": "member"
}
```

</details>
