# stove0_protocol.WorkflowPlanPayload.canonical_observations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplanpayload-canon-0e9e7ff98c:f84ffb775f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0ef94ad020"></a>
- <a id="s-dcdc45f286"></a>`distribution`: `stove0-protocol`
- <a id="s-ab8136ace5"></a>`module`: `stove0_protocol`
- <a id="s-353518fc09"></a>`name`: `canonical_observations`
- <a id="s-488ff7a5ce"></a>`owner`: `stove0_protocol.WorkflowPlanPayload`
- <a id="s-fe11ca2a90"></a>`unit`: `member`

### Declared structure

- <a id="s-a10d8ea94b"></a>`kind`: `"classmethod"`
- <a id="s-c167292e89"></a>`signature`: `"\"(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPlanPayload](stove0-protocol-workflowplanpayload.md)

## Governing policies

- <a id="pa-2e970e94ab"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlanPayload.canonical_observations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3af97ed666a1806698bb2016dcea3078bfd2fbefc561d1836a2ce28e28670eb2 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_observations",
  "owner": "stove0_protocol.WorkflowPlanPayload",
  "unit": "member"
}
```

</details>
