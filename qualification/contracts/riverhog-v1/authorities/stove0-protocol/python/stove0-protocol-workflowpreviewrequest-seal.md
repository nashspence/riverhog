# stove0_protocol.WorkflowPreviewRequest.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewrequest-seal:dba47f7efc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bfb48f6cec"></a>
- <a id="s-02847c56f9"></a>`distribution`: `stove0-protocol`
- <a id="s-2a6215107f"></a>`module`: `stove0_protocol`
- <a id="s-9b5d3dd2a6"></a>`name`: `seal`
- <a id="s-4f64df65cd"></a>`owner`: `stove0_protocol.WorkflowPreviewRequest`
- <a id="s-fcb1d78051"></a>`unit`: `member`

### Declared structure

- <a id="s-e2aa9e3172"></a>`kind`: `"classmethod"`
- <a id="s-5804f9957e"></a>`signature`: `"\"(cls, payload: 'WorkflowPreviewRequestPayload') -> 'WorkflowPreviewRequest'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreviewRequest](stove0-protocol-workflowpreviewrequest.md)

## Governing policies

- <a id="pa-80529e12a6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewRequest.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa71d0c0c40c437a910e921e6fd87f39a7cb59269bcd68e292c8b55f91abf5a3 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'WorkflowPreviewRequestPayload') -> 'WorkflowPreviewRequest'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.WorkflowPreviewRequest",
  "unit": "member"
}
```

</details>
