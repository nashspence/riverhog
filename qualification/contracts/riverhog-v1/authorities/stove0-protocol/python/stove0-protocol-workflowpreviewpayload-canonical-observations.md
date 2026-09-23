# stove0_protocol.WorkflowPreviewPayload.canonical_observations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewpayload-ca-43137b7b1f:8d0bc7a044 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4053064134"></a>
- <a id="s-3e5bbe9c1c"></a>`distribution`: `stove0-protocol`
- <a id="s-3203c0cfd5"></a>`module`: `stove0_protocol`
- <a id="s-d5d4a251e5"></a>`name`: `canonical_observations`
- <a id="s-46a6d3312f"></a>`owner`: `stove0_protocol.WorkflowPreviewPayload`
- <a id="s-5bdfd841ad"></a>`unit`: `member`

### Declared structure

- <a id="s-f15eabab30"></a>`kind`: `"classmethod"`
- <a id="s-36fd61ab32"></a>`signature`: `"\"(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreviewPayload](stove0-protocol-workflowpreviewpayload.md)

## Governing policies

- <a id="pa-bd61bc68b7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewPayload.canonical_observations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4be19f2ceaa109b07a47c87948577bf737488a45e69dc49084572b79109f1483 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_observations",
  "owner": "stove0_protocol.WorkflowPreviewPayload",
  "unit": "member"
}
```

</details>
