# stove0_protocol.WorkflowPlan.canonical_observations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplan-canonical-observations:4a42560c10 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7d28cfd7bd"></a>
- <a id="s-5f07e9d3e9"></a>`distribution`: `stove0-protocol`
- <a id="s-6f80f836c1"></a>`module`: `stove0_protocol`
- <a id="s-32144ac8a3"></a>`name`: `canonical_observations`
- <a id="s-543c4cd204"></a>`owner`: `stove0_protocol.WorkflowPlan`
- <a id="s-e8d299d99d"></a>`unit`: `member`

### Declared structure

- <a id="s-99d5e7c806"></a>`kind`: `"classmethod"`
- <a id="s-a6212cd2d7"></a>`signature`: `"\"(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPlan](stove0-protocol-workflowplan.md)

## Governing policies

- <a id="pa-eddaafd31c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlan.canonical_observations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eba872b3c1cc058912ba2367c5876ddf087324925f528a8f73ee35f63dea04b3 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_observations",
  "owner": "stove0_protocol.WorkflowPlan",
  "unit": "member"
}
```

</details>
