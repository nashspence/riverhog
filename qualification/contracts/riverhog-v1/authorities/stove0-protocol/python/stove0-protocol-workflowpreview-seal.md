# stove0_protocol.WorkflowPreview.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreview-seal:472c75eb71 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c46bc2ac7"></a>
- <a id="s-35aae3a93c"></a>`distribution`: `stove0-protocol`
- <a id="s-3b5e1f6ece"></a>`module`: `stove0_protocol`
- <a id="s-0d76bfe410"></a>`name`: `seal`
- <a id="s-7cdfcd5e50"></a>`owner`: `stove0_protocol.WorkflowPreview`
- <a id="s-1d13a32950"></a>`unit`: `member`

### Declared structure

- <a id="s-9a34fe92ea"></a>`kind`: `"classmethod"`
- <a id="s-bf469f5098"></a>`signature`: `"\"(cls, payload: 'WorkflowPreviewPayload') -> 'WorkflowPreview'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreview](stove0-protocol-workflowpreview.md)

## Governing policies

- <a id="pa-333a9d7507"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreview.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b0480a68dd3adce85787a77058634e62dfd18b454427d39a3294a2fcd877a83 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'WorkflowPreviewPayload') -> 'WorkflowPreview'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.WorkflowPreview",
  "unit": "member"
}
```

</details>
