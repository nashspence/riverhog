# stove0_core.Stove0Coordinator.inspect_coordination

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0coordinator-inspect-coordination:b8e4783065 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-58def29f21"></a>
- <a id="s-2758687c16"></a>`distribution`: `stove0-server`
- <a id="s-095ebe3191"></a>`module`: `stove0_core`
- <a id="s-4851f50104"></a>`name`: `inspect_coordination`
- <a id="s-354d182692"></a>`owner`: `stove0_core.Stove0Coordinator`
- <a id="s-5626e1e20d"></a>`unit`: `member`

### Declared structure

- <a id="s-559b645893"></a>`kind`: `"method"`
- <a id="s-afb6579cf5"></a>`signature`: `"\"(self, work_id: 'str') -> 'BranchSetEvaluation'\""`

## Maintained corroboration

### Related interface records

- [Stove0Coordinator](stove0-core-stove0coordinator.md)

## Governing policies

- <a id="pa-c2a174c5c9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0Coordinator.inspect_coordination`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b001ce98f6821b5ac509f985ab119545cda937b333183ff6ffcd9667cb641d11 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'BranchSetEvaluation'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "inspect_coordination",
  "owner": "stove0_core.Stove0Coordinator",
  "unit": "member"
}
```

</details>
